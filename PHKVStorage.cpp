#include "PHKVStorage.hpp"

#include <mutex>
#include <condition_variable>
#include <map>
#include <stdexcept>
#include <atomic>
#include <stdint.h>

#include <fmt/format.h>
#include <boost/intrusive/rbtree.hpp>
#include <boost/next_prior.hpp>
#include <boost/mpl/vector.hpp>
#include <boost/mpl/contains.hpp>

#include "StorageVolume.hpp"
#include "StringViewFormatter.hpp"
#include "LRUPriorityCachePool.hpp"
#include "KeyPathUtil.hpp"

namespace phkvs {

namespace {

std::string toString(boost::string_view sv)
{
    return {sv.data(), sv.length()};
}

bool isPrefixOf(boost::string_view prefix, boost::string_view path)
{
    if(prefix.length() > path.length())
    {
        return false;
    }
    return prefix.compare(0, prefix.length(), path.data(), prefix.length()) == 0;
}

template<class T, class ...Types>
bool isVariantType(const boost::variant<Types...>& v)
{
    using TypesVector = boost::mpl::vector<Types...>;
    static_assert(boost::mpl::contains<TypesVector, T>::type::value, "T is not variant type");
    return v.which() == boost::mpl::find<TypesVector, T>::type::pos::value;
}

struct StringStringViewComparator {
    using is_transparent = bool;

    bool operator()(const std::string& str1, const std::string& str2) const
    {
        return str1 < str2;
    }

    bool operator()(const boost::string_view& sv, const std::string& str) const
    {
        return str.compare(0, str.length(), sv.data(), sv.length()) > 0;
    }

    bool operator()(const std::string& str, const boost::string_view& sv) const
    {
        return str.compare(0, str.length(), sv.data(), sv.length()) < 0;
    }
};

class PHKVStorageImpl : public PHKVStorage {
public:
    PHKVStorageImpl(const Options& options);

    ~PHKVStorageImpl() override;

    VolumeId createAndMountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                                  boost::string_view mountPointPath) override;

    VolumeId mountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                         boost::string_view mountPointPath) override;

    void unmountVolume(VolumeId volumeId) override;

    std::vector<VolumeInfo> getMountVolumesInfo() const override;

    void store(boost::string_view keyPath, const ValueType& value, TimePointOpt expTime) override;

    boost::optional<ValueType> lookup(boost::string_view keyPath) override;

    void eraseKey(boost::string_view keyPath) override;

    void eraseDirRecursive(boost::string_view dirPath) override;

    boost::optional<std::vector<DirEntry>> getDirEntries(boost::string_view dirPath) override;

    static boost::filesystem::path
    makeMainFileFullPath(const boost::filesystem::path& volumePath, const std::string& volumeName)
    {
        auto rv = volumePath / volumeName;
        rv += ".phkvsmain";
        return rv;
    }

    static boost::filesystem::path
    makeStmFileFullPath(const boost::filesystem::path& volumePath, const std::string& volumeName)
    {
        auto rv = volumePath / volumeName;
        rv += ".phkvsstm";
        return rv;
    }

    static boost::filesystem::path
    makeBigFileFullPath(const boost::filesystem::path& volumePath, const std::string& volumeName)
    {
        auto rv = volumePath / volumeName;
        rv += ".phkvsbig";
        return rv;
    }

private:

    struct MountPointInfo {
        std::string mountPoint;
        boost::filesystem::path volumePath;
        std::string volumeName;
        VolumeId volumeId;
        uint32_t lastOpSeqAssigned = 0;
        uint32_t lastOpSeqExecuted = 0;
        bool abortOp = false;
        StorageVolume::UniquePtr volume;
        std::mutex volumeMtx;
        std::condition_variable volumeCondVar;
    };

    static FileSystem::UniqueFilePtr
    createAndCheckFile(boost::string_view callFunc, const boost::filesystem::path& path)
    {
        auto rv = FileSystem::createFileUnique(path);
        if(!rv)
        {
            int error = FileSystem::getLastError();
            throw fmt::system_error(error, "PHKVStorage::{}:Failed to create {}", callFunc, path.string());
        }

        return rv;
    }

    static FileSystem::UniqueFilePtr
    openAndCheckFile(boost::string_view callFunc, const boost::filesystem::path& path)
    {
        auto rv = FileSystem::openFileUnique(path);
        if(!rv)
        {
            int error = FileSystem::getLastError();
            throw fmt::system_error(error, "PHKVStorage::{}:Failed to open {}", callFunc, path.string());
        }

        return rv;
    }

    using LockGuard = std::lock_guard<std::mutex>;
    using UniqueLock = std::unique_lock<std::mutex>;
    using MountPointInfoPtr = std::shared_ptr<MountPointInfo>;

    struct MountTree {
        std::map<VolumeId, MountPointInfoPtr> mountPoints;
        using SubdirsMap = std::map<std::string, MountTree, StringStringViewComparator>;
        SubdirsMap subdirs;
        size_t childMounts = 0;
    };

    mutable std::mutex m_mountInfoMtx;
    MountTree m_mountTree;
    std::map<VolumeId, MountPointInfoPtr> m_volumeIdMap;
    VolumeId m_lastVolumeId{0};

    VolumeId registerMount(const boost::string_view& mountPath, MountPointInfoPtr infoPtr);

    void erasePathFromMountTree(MountTree& subtree, const std::vector<boost::string_view>& mountPath,
                                size_t idx, VolumeId volumeId);

    std::tuple<MountPointInfoPtr, uint32_t> getVolumeByIdAndAllocateOpSeq(VolumeId volumeId);

    uint32_t acquireVolumeOpSeq(MountPointInfo& mount);

    void executeOpInSequence(MountPointInfo& mnt, uint32_t opSeq, const std::function<void()>& op);

    static void waitForPendingOps(MountPointInfo& mnt, UniqueLock& lock);

    static boost::string_view getLocalMountPath(const boost::string_view& fullPath, MountPointInfo& mnt);

    struct VolumeNotFound {
    };
    using FoundVolumes = boost::variant<VolumeNotFound, MountPointInfoPtr, std::vector<MountPointInfoPtr>>;

    std::vector<MountPointInfoPtr> findVolumesByPath(boost::string_view keyPath);

    void getVolumesFromTree(const MountTree& tree, const std::vector<boost::string_view>& path, size_t idx,
                            std::vector<MountPointInfoPtr>& volumes);

    std::atomic_uint_fast32_t m_cacheSeq{0};

    struct CacheTreeNode;

    struct CacheNodeComparator {
        bool operator()(const CacheTreeNode& l, const CacheTreeNode& r) const;

        template<typename StringType>
        bool operator()(const CacheTreeNode& l, const StringType& rname) const;

        template<typename StringType>
        bool operator()(const StringType& lname, const CacheTreeNode& r) const;
    };

    struct CacheTreeNode : boost::intrusive::set_base_hook<> {

        boost::intrusive::list_member_hook<> poolListNode;
        uint8_t poolPrio;

        using CacheTree = boost::intrusive::rbtree<CacheTreeNode, boost::intrusive::compare<CacheNodeComparator>>;

        struct Dir {
            CacheTree content;
            bool overlapingDir = false;
            bool cacheComplete = false;

            CacheTreeNode* find(boost::string_view name);

            void erase(CacheTreeNode* node);
        };

        EntryType type;
        uint32_t cacheSeq;
        VolumeId volumeId;
        std::string name;
        boost::variant<ValueType, Dir> value;
        CacheTreeNode* parent;

        Dir& getDir()
        {
            return boost::get<Dir>(value);
        }

        ValueType& getValue()
        {
            return boost::get<ValueType>(value);
        }

        void clear()
        {
            if(type == EntryType::dir)
            {
                auto& dir = getDir();
                for(auto& node : dir.content)
                {
                    node.parent = nullptr;
                    node.clear();
                }
                dir.content.clear();
            }
        }
    };

    void initDirCacheNode(CacheTreeNode& node, std::string&& name, CacheTreeNode* parent)
    {
        node.type = EntryType::dir;
        node.cacheSeq = m_cacheSeq.load(std::memory_order_acquire);
        node.name = std::move(name);
        node.value = CacheTreeNode::Dir();
        node.parent = parent;
    }

    void initValueCacheNode(CacheTreeNode& node, std::string&& name, const ValueType& value, VolumeId volumeId,
                            CacheTreeNode* parent)
    {
        node.type = EntryType::key;
        node.cacheSeq = m_cacheSeq.load(std::memory_order_acquire);
        node.name = std::move(name);
        node.value = value;
        node.volumeId = volumeId;
        node.parent = parent;
    }

    bool isActualCacheDirNode(CacheTreeNode& node)
    {
        return node.type == EntryType::dir &&
               node.cacheSeq == m_cacheSeq.load(std::memory_order_acquire) &&
               node.getDir().cacheComplete;
    }

    bool isActualCacheKeyNode(CacheTreeNode& node)
    {
        return node.type == EntryType::key &&
               node.cacheSeq == m_cacheSeq.load(std::memory_order_acquire);
    }

    mutable std::mutex m_cacheMtx;
    using CachePoolType = LRUPriorityCachePool<CacheTreeNode, &CacheTreeNode::poolListNode, &CacheTreeNode::poolPrio, 2>;
    CachePoolType m_cachePool;
    CacheTreeNode* m_cacheRoot;

    enum class FindResult {
        found,
        notFound,
        logicError,
        inconsistentCache
    };

    std::tuple<FindResult, CacheTreeNode*> findInCache(const std::vector<boost::string_view>& path);

    void storeInCache(const PathAndKey& pathKey, const ValueType& value, VolumeId volumeId, uint8_t prio);

    void fillCache(const std::vector<boost::string_view>& path);

    void eraseFromCache(CacheTreeNode* dirNode, CacheTreeNode* childNode);

    void cacheNodeReuseNotify(CacheTreeNode* node);
};

bool PHKVStorageImpl::CacheNodeComparator::operator()(const PHKVStorageImpl::CacheTreeNode& l,
                                                      const PHKVStorageImpl::CacheTreeNode& r) const
{
    return l.name < r.name;
}

template<typename StringType>
bool
PHKVStorageImpl::CacheNodeComparator::operator()(const PHKVStorageImpl::CacheTreeNode& l, const StringType& rname) const
{
    return l.name < rname;
}

template<typename StringType>
bool
PHKVStorageImpl::CacheNodeComparator::operator()(const StringType& lname, const PHKVStorageImpl::CacheTreeNode& r) const
{
    return lname < r.name;
}


PHKVStorageImpl::CacheTreeNode* PHKVStorageImpl::CacheTreeNode::Dir::find(boost::string_view name)
{
    auto it = content.find(name, CacheNodeComparator());
    if(it == content.end())
    {
        return nullptr;
    }
    return &*it;
}

void PHKVStorageImpl::CacheTreeNode::Dir::erase(CacheTreeNode* node)
{
    content.erase(content.iterator_to(*node));
}

PHKVStorageImpl::PHKVStorageImpl(const Options& options) :
        m_cachePool(options.cachePoolSize,
                std::bind(&PHKVStorageImpl::cacheNodeReuseNotify, this, std::placeholders::_1))
{
    m_cacheRoot = m_cachePool.allocate(0);
    initDirCacheNode(*m_cacheRoot, "", nullptr);
}

PHKVStorageImpl::~PHKVStorageImpl()
{
    m_cacheRoot->clear();
}

void PHKVStorageImpl::cacheNodeReuseNotify(CacheTreeNode* node)
{
    if(node->parent)
    {
        node->parent->getDir().content.erase(node->parent->getDir().content.iterator_to(*node));
        node->parent->getDir().cacheComplete = false;
    }
}

PHKVStorageImpl::VolumeId
PHKVStorageImpl::createAndMountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                                      boost::string_view mountPointPath)
{
    if(!boost::filesystem::exists(volumePath))
    {
        boost::filesystem::create_directories(volumePath);
    }
    std::string volumeNameStr = toString(volumeName);
    auto mainPath = makeMainFileFullPath(volumePath, volumeNameStr);
    auto stmPath = makeStmFileFullPath(volumePath, volumeNameStr);
    auto bigPath = makeBigFileFullPath(volumePath, volumeNameStr);
    for(auto pathPtr:{&mainPath, &stmPath, &bigPath})
    {
        if(boost::filesystem::exists(*pathPtr))
        {
            throw std::runtime_error(fmt::format("PHKVStorage::createAndMountVolume: File {} already exists.",
                    pathPtr->string()));
        }
    }
    auto volume = StorageVolume::create(createAndCheckFile("PHKVStorage::createAndMountVolume", mainPath),
            SmallToMediumFileStorage::create(createAndCheckFile("PHKVStorage::createAndMountVolume", stmPath)),
            BigFileStorage::create(createAndCheckFile("PHKVStorage::createAndMountVolume", bigPath)));

    auto infoPtr = std::make_shared<MountPointInfo>();

    auto& info = *infoPtr;
    info.volume = std::move(volume);
    info.volumeName = volumeNameStr;
    info.volumePath = volumePath;
    return registerMount(mountPointPath, infoPtr);
}

PHKVStorageImpl::VolumeId
PHKVStorageImpl::mountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                             boost::string_view mountPointPath)
{
    std::string volumeNameStr = toString(volumeName);
    auto mainPath = makeMainFileFullPath(volumePath, volumeNameStr);
    auto stmPath = makeStmFileFullPath(volumePath, volumeNameStr);
    auto bigPath = makeBigFileFullPath(volumePath, volumeNameStr);
    for(auto pathPtr:{&mainPath, &stmPath, &bigPath})
    {
        if(!boost::filesystem::exists(*pathPtr))
        {
            throw std::runtime_error(fmt::format("PHKVStorage::createAndMountVolume: File {} doesn't exists.",
                    pathPtr->string()));
        }
    }
    auto volume = StorageVolume::open(openAndCheckFile("PHKVStorage::mountVolume", mainPath),
            SmallToMediumFileStorage::open(openAndCheckFile("PHKVStorage::mountVolume", stmPath)),
            BigFileStorage::open(openAndCheckFile("PHKVStorage::mountVolume", bigPath)));

    auto infoPtr = std::make_shared<MountPointInfo>();
    auto& info = *infoPtr;
    info.volume = std::move(volume);
    info.volumeName = volumeNameStr;
    info.volumePath = volumePath;

    return registerMount(mountPointPath, infoPtr);
}

void PHKVStorageImpl::unmountVolume(VolumeId volumeId)
{
    LockGuard guard(m_mountInfoMtx);
    auto it = m_volumeIdMap.find(volumeId);
    if(it == m_volumeIdMap.end())
    {
        return;
    }

    auto& info = *it->second;
    auto path = splitDirPath(info.mountPoint);
    erasePathFromMountTree(m_mountTree, path, 0, info.volumeId);
    m_volumeIdMap.erase(it);

    m_cacheSeq.fetch_add(1, std::memory_order_release);
}

void
PHKVStorageImpl::erasePathFromMountTree(MountTree& subtree, const std::vector<boost::string_view>& mountPath,
                                        size_t idx, VolumeId volumeId)
{
    if(idx == mountPath.size())
    {
        subtree.mountPoints.erase(volumeId);
        return;
    }
    auto it = subtree.subdirs.find(mountPath[idx]);
    if(it == subtree.subdirs.end())
    {
        //report error?
        return;
    }
    auto& nextTree = it->second;
    erasePathFromMountTree(it->second, mountPath, idx + 1, volumeId);
    --subtree.childMounts;
    if(nextTree.mountPoints.empty() && nextTree.subdirs.empty())
    {
        subtree.subdirs.erase(it);
    }
}

std::vector<PHKVStorageImpl::VolumeInfo> PHKVStorageImpl::getMountVolumesInfo() const
{
    LockGuard guard(m_mountInfoMtx);
    std::vector<VolumeInfo> rv;
    for(auto& p : m_volumeIdMap)
    {
        const MountPointInfo& mp = *p.second;
        rv.push_back({mp.volumePath, mp.volumeName, mp.mountPoint, mp.volumeId});
    }
    return rv;
}

std::tuple<PHKVStorageImpl::MountPointInfoPtr, uint32_t>
PHKVStorageImpl::getVolumeByIdAndAllocateOpSeq(VolumeId volumeId)
{
    LockGuard guard(m_mountInfoMtx);
    auto it = m_volumeIdMap.find(volumeId);
    if(it == m_volumeIdMap.end())
    {
        return {};
    }
    auto rv = it->second;
    return {rv, ++rv->lastOpSeqAssigned};
}

uint32_t PHKVStorageImpl::acquireVolumeOpSeq(MountPointInfo& mount)
{
    LockGuard guard(m_mountInfoMtx);
    return ++mount.lastOpSeqAssigned;
}

PHKVStorageImpl::VolumeId
PHKVStorageImpl::registerMount(const boost::string_view& mountPointPath, MountPointInfoPtr infoPtr)
{
    LockGuard guard(m_mountInfoMtx);
    m_cacheSeq.fetch_add(1, std::memory_order_release);
    auto rv = ++m_lastVolumeId;

    auto& info = *infoPtr;
    info.volumeId = rv;
    info.mountPoint = toString(mountPointPath);
    m_volumeIdMap.emplace(info.volumeId, infoPtr);

    auto path = splitDirPath(mountPointPath);

    MountTree* node = &m_mountTree;
    for(auto& item: path)
    {
        ++node->childMounts;
        auto it = node->subdirs.find(item);
        if(it == node->subdirs.end())
        {
            it = node->subdirs.emplace(item, MountTree()).first;
        }
        node = &it->second;
    }
    node->mountPoints.emplace(infoPtr->volumeId, infoPtr);

    return rv;
}

void PHKVStorageImpl::executeOpInSequence(MountPointInfo& mnt, uint32_t opSeq, const std::function<void()>& op)
{
    UniqueLock lock(mnt.volumeMtx);
    while(opSeq - mnt.lastOpSeqExecuted != 1 && !mnt.abortOp)
    {
        mnt.volumeCondVar.wait(lock);
    }
    if(mnt.abortOp)
    {
        return;
    }
    try
    {
        op();
    }
    catch(...)
    {
        mnt.lastOpSeqExecuted = opSeq;
        mnt.volumeCondVar.notify_all();
        m_cacheSeq.fetch_add(1, std::memory_order_release);
        throw;
    }
    mnt.lastOpSeqExecuted = opSeq;
    mnt.volumeCondVar.notify_all();
}

void PHKVStorageImpl::waitForPendingOps(MountPointInfo& mnt, UniqueLock& lock)
{
    while(mnt.lastOpSeqAssigned != mnt.lastOpSeqExecuted && !mnt.abortOp)
    {
        mnt.volumeCondVar.wait(lock);
    }
}

boost::string_view PHKVStorageImpl::getLocalMountPath(const boost::string_view& fullPath, MountPointInfo& mnt)
{
    boost::string_view fullPathSv(fullPath);
    return fullPathSv.substr(mnt.mountPoint.length());
}

std::vector<PHKVStorageImpl::MountPointInfoPtr> PHKVStorageImpl::findVolumesByPath(boost::string_view keyPath)
{
    LockGuard guard(m_mountInfoMtx);
    if(m_volumeIdMap.empty())
    {
        return {};
    }

    auto pathKey = splitKeyPath(keyPath);


    std::vector<MountPointInfoPtr> rv;
    getVolumesFromTree(m_mountTree, pathKey.path, 0, rv);

    std::sort(rv.begin(), rv.end(),
            [](const MountPointInfoPtr& l, const MountPointInfoPtr& r) { return l->volumeId < r->volumeId; });
    return rv;
}

void PHKVStorageImpl::getVolumesFromTree(const MountTree& tree, const std::vector<boost::string_view>& path, size_t idx,
                                         std::vector<MountPointInfoPtr>& volumes)
{
    for(auto& p : tree.mountPoints)
    {
        volumes.push_back(p.second);
    }
    if(idx < path.size())
    {
        auto it = tree.subdirs.find(path[idx]);
        if(it != tree.subdirs.end())
        {
            getVolumesFromTree(it->second, path, idx + 1, volumes);
        }
    }
}


std::tuple<PHKVStorageImpl::FindResult, PHKVStorageImpl::CacheTreeNode*>
PHKVStorageImpl::findInCache(const std::vector<boost::string_view>& path)
{
    CacheTreeNode* node = m_cacheRoot;
    for(auto& item: path)
    {
        if(node->cacheSeq != m_cacheSeq.load(std::memory_order_acquire))
        {
            return {FindResult::inconsistentCache, nullptr};
        }
        if(node->type == EntryType::key)
        {
            return {FindResult::logicError, nullptr};
        }
        auto& dir = node->getDir();
        node = dir.find(item);
        if(!node)
        {
            return {dir.cacheComplete ? FindResult::notFound : FindResult::inconsistentCache, nullptr};
        }
        m_cachePool.touch(node);
    }
    if(node->cacheSeq != m_cacheSeq.load(std::memory_order_acquire))
    {
        return {FindResult::inconsistentCache, nullptr};
    }
    if(node->type == EntryType::key)
    {
        return {FindResult::logicError, nullptr};
    }
    return {node->getDir().cacheComplete ? FindResult::found : FindResult::inconsistentCache, node};
}

void
PHKVStorageImpl::storeInCache(const PathAndKey& pathKey, const ValueType& value, VolumeId volumeId, uint8_t prio)
{
    CacheTreeNode* node = m_cacheRoot;
    for(auto& item:pathKey.path)
    {
        if(node->type != EntryType::dir)
        {
            //error?
            return;
        }
        m_cachePool.touch(node);
        auto nextNode = node->getDir().find(item);
        if(nextNode)
        {
            node = nextNode;
        }
        else
        {
            auto newNode = m_cachePool.allocate(prio);
            initDirCacheNode(*newNode, toString(item), node);
            node->getDir().content.insert_unique(*newNode);
            node = newNode;
        }
    }
    if(node->type != EntryType::dir)
    {
        //error?
        return;
    }
    auto keyNode = node->getDir().find(pathKey.key);
    if(!keyNode)
    {
        auto newNode = m_cachePool.allocate(prio);
        initValueCacheNode(*newNode, toString(pathKey.key), value, volumeId, node);
        node->getDir().content.insert_unique(*newNode);
    }
    else
    {
        keyNode->getValue() = value;
    }
}

void PHKVStorageImpl::fillCache(const std::vector<boost::string_view>& path)
{
    //LockGuard guardCache(m_cacheMtx);
    LockGuard guardMount(m_mountInfoMtx);
    MountTree* mountNode = &m_mountTree;
    CacheTreeNode* cacheNode = m_cacheRoot;
    std::string fullPath = "/";
    size_t idx = 0;
    std::string tempKeyPath;
    bool mountFollowingPath = true;
    do
    {
        if(!isActualCacheDirNode(*cacheNode))
        {
            cacheNode->clear();
            cacheNode->cacheSeq = m_cacheSeq.load(std::memory_order_acquire);
            cacheNode->getDir().cacheComplete = true;
            cacheNode->getDir().overlapingDir = mountNode->childMounts > 1;
            for(auto& p:mountNode->mountPoints)
            {
                MountPointInfo& mountPoint = *p.second;
                UniqueLock lock(mountPoint.volumeMtx);
                waitForPendingOps(mountPoint, lock);
                auto dir = mountPoint.volume->getDirEntries(getLocalMountPath(fullPath, mountPoint));

                if(dir)
                {
                    for(auto& dirEntry:*dir)
                    {
                        auto& cacheDir = cacheNode->getDir();
                        auto node = cacheDir.find(dirEntry.name);
                        if(!node)
                        {
                            auto newCacheNode = m_cachePool.allocate(mountNode->childMounts > 1 ? 0 : 1);
                            if(dirEntry.type == EntryType::key)
                            {
                                tempKeyPath = toString(getLocalMountPath(fullPath, mountPoint));
                                tempKeyPath += "/";
                                tempKeyPath += dirEntry.name;
                                auto val = mountPoint.volume->lookup(tempKeyPath);
                                if(val)
                                {
                                    initValueCacheNode(*newCacheNode,
                                            std::move(dirEntry.name),
                                            *val,
                                            mountPoint.volumeId,
                                            cacheNode);
                                }
                            }
                            else
                            {
                                initDirCacheNode(*newCacheNode, std::move(dirEntry.name), cacheNode);
                            }
                            cacheDir.content.insert_unique(*newCacheNode);
                        }
                        else
                        {
                            if(dirEntry.type == EntryType::key)
                            {
                                if(!isActualCacheKeyNode(*node))
                                {
                                    tempKeyPath = toString(getLocalMountPath(fullPath, mountPoint));
                                    tempKeyPath += "/";
                                    tempKeyPath += dirEntry.name;
                                    auto val = mountPoint.volume->lookup(tempKeyPath);
                                    initValueCacheNode(*node,
                                            std::move(dirEntry.name),
                                            *val,
                                            mountPoint.volumeId,
                                            cacheNode);
                                }
                            }
                            else
                            {
                                if(!isActualCacheDirNode(*node))
                                {
                                    initDirCacheNode(*node, std::move(dirEntry.name), cacheNode);
                                }
                            }
                            m_cachePool.touch(node);
                        }
                        if(!cacheNode->getDir().cacheComplete)
                        {
                            //not enough cache size to load directory
                            return;
                        }
                    }
                }
            }
            if(mountFollowingPath)
            {
                for(auto& p : mountNode->subdirs)
                {
                    auto node = cacheNode->getDir().find(p.first);
                    if(!node)
                    {
                        auto newCacheNode = m_cachePool.allocate(mountNode->childMounts > 1 ? 0 : 1);
                        initDirCacheNode(*newCacheNode, std::string(p.first), cacheNode);
                        cacheNode->getDir().content.insert_unique(*newCacheNode);
                    }
                }
            }
        }
        if(idx < path.size())
        {
            auto& item = path[idx];
            if(mountFollowingPath)
            {
                auto it = mountNode->subdirs.find(item);
                if(it != mountNode->subdirs.end())
                {
                    mountNode = &it->second;
                }
                else
                {
                    mountFollowingPath = false;
                }
            }
            {
                auto node = cacheNode->getDir().find(item);
                if(!node)
                {
                    auto newCacheNode = m_cachePool.allocate(mountNode->childMounts > 1 ? 0 : 1);
                    initDirCacheNode(*newCacheNode, toString(item), cacheNode);
                    cacheNode->getDir().content.insert_unique(*newCacheNode);
                    cacheNode = newCacheNode;
                }
                else
                {
                    cacheNode = node;
                }
            }
            fullPath.append(item.data(), item.length());
            fullPath += '/';
        }
        ++idx;
    } while(idx <= path.size());
}

void PHKVStorageImpl::eraseFromCache(CacheTreeNode* dirNode, CacheTreeNode* childNode)
{
    dirNode->getDir().erase(childNode);
    m_cachePool.free(childNode);
    if(dirNode->getDir().content.empty() && dirNode->parent)
    {
        eraseFromCache(dirNode->parent, dirNode);
    }
}

void PHKVStorageImpl::store(boost::string_view keyPath, const ValueType& value, TimePointOpt expTime)
{
    auto pathKey = splitKeyPath(keyPath);
    MountPointInfoPtr mount;
    uint32_t volumeOpSeq;
    {
        LockGuard guard(m_cacheMtx);
        FindResult result;
        CacheTreeNode* node;
        std::tie(result, node) = findInCache(pathKey.path);
        if(result == FindResult::inconsistentCache)
        {
            fillCache(pathKey.path);
            std::tie(result, node) = findInCache(pathKey.path);
        }


        if(node)
        {
            auto keyNode = node->getDir().find(pathKey.key);
            if(keyNode)
            {
                keyNode->value = value;
                m_cachePool.touch(keyNode);
                std::tie(mount, volumeOpSeq) = getVolumeByIdAndAllocateOpSeq(keyNode->volumeId);
            }
        }
        if(!mount)
        {
            auto volumes = findVolumesByPath(keyPath);
            if(volumes.empty())
            {
                throw std::runtime_error(fmt::format("No volumes were mount for path {}", keyPath));
            }

            uint8_t prio = volumes.size() > 1 ? 0 : 1;
            mount = volumes.front();
            storeInCache(pathKey, value, mount->volumeId, prio);
            volumeOpSeq = acquireVolumeOpSeq(*mount);
        }
    }
    executeOpInSequence(*mount, volumeOpSeq, [&mount, keyPath, &value, expTime]() {
        mount->volume->store(getLocalMountPath(keyPath, *mount), value, expTime);
    });
}

boost::optional<PHKVStorageImpl::ValueType> PHKVStorageImpl::lookup(boost::string_view keyPath)
{
    auto pathKey = splitKeyPath(keyPath);
    bool isCacheComplete = true;
    {
        LockGuard guard(m_cacheMtx);
        FindResult result;
        CacheTreeNode* node;
        std::tie(result, node) = findInCache(pathKey.path);
        if(result == FindResult::inconsistentCache)
        {
            fillCache(pathKey.path);
            std::tie(result, node) = findInCache(pathKey.path);
        }

        if(node)
        {
            auto keyNode = node->getDir().find(pathKey.key);
            if(keyNode && isActualCacheKeyNode(*keyNode))
            {
                m_cachePool.touch(&*keyNode);
                return keyNode->getValue();
            }
            isCacheComplete = node->getDir().cacheComplete;
        }
    }
    if(!isCacheComplete)
    {
        auto volumes = findVolumesByPath(keyPath);
        for(auto& vol:volumes)
        {
            LockGuard guard(vol->volumeMtx);
            auto rv = vol->volume->lookup(getLocalMountPath(keyPath, *vol));
            if(rv)
            {
                return rv;
            }
        }
    }
    return {};
}

void PHKVStorageImpl::eraseKey(boost::string_view keyPath)
{
    auto pathKey = splitKeyPath(keyPath);
    MountPointInfoPtr mount;
    uint32_t volumeOpSeq;
    {
        LockGuard guard(m_cacheMtx);
        FindResult result;
        CacheTreeNode* node;
        std::tie(result, node) = findInCache(pathKey.path);
        if(result == FindResult::inconsistentCache)
        {
            fillCache(pathKey.path);
            std::tie(result, node) = findInCache(pathKey.path);
        }

        if(result == FindResult::found)
        {
            auto keyNode = node->getDir().find(pathKey.key);
            if(keyNode && isActualCacheKeyNode(*keyNode))
            {
                std::tie(mount, volumeOpSeq) = getVolumeByIdAndAllocateOpSeq(keyNode->volumeId);
                eraseFromCache(node, keyNode);
            }
        }
    }
    if(mount)
    {
        executeOpInSequence(*mount, volumeOpSeq, [&mount, keyPath]() {
            mount->volume->eraseKey(keyPath);
        });
    }
}

void PHKVStorageImpl::eraseDirRecursive(boost::string_view dirPath)
{
    auto path = splitDirPath(dirPath);
    std::vector<std::pair<MountPointInfoPtr, uint32_t>> mops;
    {
        LockGuard guard(m_cacheMtx);
        FindResult result;
        CacheTreeNode* node;
        std::tie(result, node) = findInCache(path);
        if(result == FindResult::inconsistentCache)
        {
            fillCache(path);
            std::tie(result, node) = findInCache(path);
        }

        if(result == FindResult::found)
        {
            if(node->parent)
            {
                node->clear();
                eraseFromCache(node->parent, node);
            }
        }
        auto volumes = findVolumesByPath(dirPath);
        if(volumes.empty())
        {
            throw std::runtime_error(fmt::format("No volumes were mount for path {}", dirPath));
        }
        for(auto& mp: volumes)
        {
            mops.emplace_back(mp, acquireVolumeOpSeq(*mp));
        }
    }
    for(auto& mp:mops)
    {
        auto mount = mp.first;
        auto opSeq = mp.second;
        executeOpInSequence(*mount, opSeq, [dirPath, &mount]() {
            mount->volume->eraseDirRecursive(getLocalMountPath(dirPath, *mount));
        });
    }
}

boost::optional<std::vector<PHKVStorageImpl::DirEntry>> PHKVStorageImpl::getDirEntries(boost::string_view dirPath)
{
    auto path = splitDirPath(dirPath);

    LockGuard guard(m_cacheMtx);
    FindResult result;
    CacheTreeNode* node;
    std::tie(result, node) = findInCache(path);
    if(result == FindResult::inconsistentCache)
    {
        fillCache(path);
        std::tie(result, node) = findInCache(path);
    }
    std::vector<PHKVStorageImpl::DirEntry> rv;
    if(result == FindResult::found)
    {
        auto& dir = node->getDir();
        for(auto& childNode:dir.content)
        {
            rv.push_back({childNode.type, childNode.name});
        }
        return {rv};
    }
    return {};
}

}

PHKVStorage::UniquePtr PHKVStorage::create(const Options& options)
{
    return std::make_unique<PHKVStorageImpl>(options);
}

void PHKVStorage::deleteVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName)
{
    boost::filesystem::remove(PHKVStorageImpl::makeMainFileFullPath(volumePath, toString(volumeName)));
    boost::filesystem::remove(PHKVStorageImpl::makeStmFileFullPath(volumePath, toString(volumeName)));
    boost::filesystem::remove(PHKVStorageImpl::makeBigFileFullPath(volumePath, toString(volumeName)));
}

}
