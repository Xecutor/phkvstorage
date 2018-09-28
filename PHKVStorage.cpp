#include "PHKVStorage.hpp"

#include <mutex>
#include <map>
#include <stdexcept>
#include <atomic>
#include <stdint.h>

#include <fmt/format.h>
#include <boost/intrusive/rbtree.hpp>
#include <boost/next_prior.hpp>

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

struct StringStringViewComparator {
    bool operator()(const std::string& str1, const std::string& str2)
    {
        return str1 < str2;
    }

    bool operator()(const boost::string_view& sv, const std::string& str)
    {
        return str.compare(0, str.length(), sv.data(), sv.length()) > 0;
    }

    bool operator()(const std::string& str, const boost::string_view& sv)
    {
        return str.compare(0, str.length(), sv.data(), sv.length()) < 0;
    }
};

class PHKVStorageImpl : public PHKVStorage {
public:
    PHKVStorageImpl(const Options& options);

    VolumeId createAndMountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                                  boost::string_view mountPointPath) override;

    VolumeId mountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                         boost::string_view mountPointPath) override;

    void unmountVolume(VolumeId volumeId) override;

    std::vector<VolumeInfo> getMountVolumesInfo() const override;

    void store(boost::string_view keyPath, const ValueType& value) override;

    void storeExpiring(boost::string_view keyPath, const ValueType& value, TimePoint expTime) override;

    boost::optional<ValueType> lookup(boost::string_view keyPath) override;

    void eraseKey(boost::string_view keyPath) override;

    void eraseDirRecursive(boost::string_view dirPath) override;

    boost::optional<std::vector<DirEntry>> getDirEntries(boost::string_view dirPath) override;

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
    using MountPointInfoPtr = std::shared_ptr<MountPointInfo>;
    using MountPointsMap = std::multimap<std::string, MountPointInfoPtr>;
    mutable std::mutex m_mountInfoMtx;
    MountPointsMap m_mountPoints;
    VolumeId m_lastVolumeId{0};
    std::map<VolumeId, MountPointsMap::iterator> m_volumeIdMap;

    std::tuple<MountPointInfoPtr, uint32_t> getVolumeByIdAndAllocateOpSeq(VolumeId volumeId);
    void executeOpInSequence(MountPointInfo& mnt, uint32_t opSeq, std::function<void()> op);

    struct VolumeNotFound {
    };
    using FoundVolumes = boost::variant<VolumeNotFound, MountPointInfoPtr, std::vector<MountPointInfoPtr>>;

    FoundVolumes findVolumeByPath(boost::string_view path);

    std::atomic_uint_fast32_t m_cacheSeq{0};

    struct CacheTreeNode : boost::intrusive::set_base_hook<> {

        boost::intrusive::list_member_hook<> poolListNode;
        uint8_t poolPrio;

        using CacheTree = boost::intrusive::rbtree<CacheTreeNode>;
        struct Dir {
            CacheTree tree;
            bool overlapingDir = false;
            bool cacheComplete = false;
        };

        EntryType type;
        std::string name;
        uint32_t cacheSeq;
        VolumeId volumeId;
        boost::variant<ValueType, Dir> value;

        Dir& getDir()
        {
            return boost::get<Dir>(value);
        }

        ValueType& getValue()
        {
            return boost::get<ValueType>(value);
        }
    };

    struct CacheNodeComparator {
        bool operator()(const CacheTreeNode& l, const CacheTreeNode& r) const
        {
            return l.name < r.name;
        }

        bool operator()(const CacheTreeNode& l, boost::string_view rname) const
        {
            return l.name < rname;
        }

        bool operator()(boost::string_view lname, const CacheTreeNode& r) const
        {
            return lname < r.name;
        }
    };

    mutable std::mutex m_cacheMtx;
    using CachePoolType = LRUPriorityCachePool<CacheTreeNode, &CacheTreeNode::poolListNode, &CacheTreeNode::poolPrio, 2>;
    CachePoolType m_cachePool;
    CacheTreeNode m_cacheRoot;

    CacheTreeNode* findInCache(const std::vector<boost::string_view>& path);

    void cacheNodeReuseNotify(CacheTreeNode* node);
};

PHKVStorageImpl::PHKVStorageImpl(const Options& options) :
        m_cachePool(options.cachePoolSize,
                std::bind(&PHKVStorageImpl::cacheNodeReuseNotify, this, std::placeholders::_1))
{
}

void PHKVStorageImpl::cacheNodeReuseNotify(CacheTreeNode* node)
{

}

PHKVStorageImpl::VolumeId
PHKVStorageImpl::createAndMountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                                      boost::string_view mountPointPath)
{
    LockGuard lg(m_mountInfoMtx);
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
    auto volume = StorageVolume::create(createAndCheckFile(__FUNCTION__, mainPath),
            SmallToMediumFileStorage::create(createAndCheckFile(__FUNCTION__, stmPath)),
            BigFileStorage::create(createAndCheckFile(__FUNCTION__, bigPath)));

    auto rv = ++m_lastVolumeId;

    auto infoPtr = std::make_shared<MountPointInfo>();
    auto it = m_mountPoints.emplace(toString(mountPointPath), infoPtr);
    auto& info = *infoPtr;
    info.volumeId = rv;
    info.volume = std::move(volume);
    info.mountPoint = it->first;
    info.volumeName = volumeNameStr;
    info.volumePath = volumePath;

    m_volumeIdMap.emplace(info.volumeId, it);

    return rv;
}

PHKVStorageImpl::VolumeId
PHKVStorageImpl::mountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                             boost::string_view mountPointPath)
{
    LockGuard lg(m_mountInfoMtx);
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
    auto volume = StorageVolume::create(openAndCheckFile(__FUNCTION__, mainPath),
            SmallToMediumFileStorage::create(openAndCheckFile(__FUNCTION__, stmPath)),
            BigFileStorage::create(openAndCheckFile(__FUNCTION__, bigPath)));

    auto rv = ++m_lastVolumeId;

    auto infoPtr = std::make_shared<MountPointInfo>();
    auto it = m_mountPoints.emplace(toString(mountPointPath), infoPtr);
    auto& info = *infoPtr;
    info.volumeId = rv;
    info.volume = std::move(volume);
    info.mountPoint = it->first;
    info.volumeName = volumeNameStr;
    info.volumePath = volumePath;

    m_volumeIdMap.emplace(info.volumeId, it);

    return rv;
}

void PHKVStorageImpl::unmountVolume(VolumeId volumeId)
{
    LockGuard lg(m_mountInfoMtx);
    auto it = m_volumeIdMap.find(volumeId);
    if(it == m_volumeIdMap.end())
    {
        return;
    }
    m_mountPoints.erase(it->second);
    m_volumeIdMap.erase(it);
    ++m_cacheSeq;
}

std::vector<PHKVStorageImpl::VolumeInfo> PHKVStorageImpl::getMountVolumesInfo() const
{
    LockGuard lg(m_mountInfoMtx);
    std::vector<VolumeInfo> rv;
    for(auto& p : m_mountPoints)
    {
        const MountPointInfo& mp = *p.second;
        rv.push_back({mp.volumePath, mp.volumeName, mp.mountPoint, mp.volumeId});
    }
    return rv;
}

std::tuple<PHKVStorageImpl::MountPointInfoPtr, uint32_t> PHKVStorageImpl::getVolumeByIdAndAllocateOpSeq(VolumeId volumeId)
{
    LockGuard guard(m_mountInfoMtx);
    auto it = m_volumeIdMap.find(volumeId);
    if(it == m_volumeIdMap.end())
    {
        return {};
    }
    auto rv = it->second->second;
    return {rv, ++rv->lastOpSeqAssigned};
}

void PHKVStorageImpl::executeOpInSequence(MountPointInfo& mnt, uint32_t opSeq, std::function<void()> op)
{
    std::unique_lock<std::mutex> lock(mnt.volumeMtx);
    while(opSeq - mnt.lastOpSeqExecuted != 1 && !mnt.abortOp)
    {
        mnt.volumeCondVar.wait(lock);
    }
    if(mnt.abortOp)
    {
        return;
    }
    op();
    mnt.lastOpSeqExecuted = opSeq;
}

PHKVStorageImpl::FoundVolumes PHKVStorageImpl::findVolumeByPath(boost::string_view path)
{
    LockGuard lg(m_mountInfoMtx);
    if(m_mountPoints.empty())
    {
        return VolumeNotFound();
    }
    auto end = m_mountPoints.lower_bound(toString(path));
    auto it = end;
    while(it != m_mountPoints.begin())
    {
        auto prev = boost::prior(it);
        if(isPrefixOf(prev->first, path))
        {
            it = prev;
        }
        else
        {
            break;
        }
    }
    if(it == m_mountPoints.end() || it == end || !isPrefixOf(it->first, path))
    {
        return VolumeNotFound();
    }
    if(boost::next(it) == end)
    {
        return {it->second};
    }

    std::vector<MountPointInfoPtr> rv;
    for(; it != end; ++it)
    {
        rv.push_back(it->second);
    }
    std::sort(rv.begin(), rv.end(), [](const MountPointInfoPtr& l, const MountPointInfoPtr& r){return l->volumeId < r->volumeId;});
    return {rv};
}

PHKVStorageImpl::CacheTreeNode* PHKVStorageImpl::findInCache(const std::vector<boost::string_view>& path)
{
    CacheTreeNode* node = &m_cacheRoot;
    for(auto& item: path)
    {
        if(node->cacheSeq != m_cacheSeq)
        {
            return nullptr;
        }
        if(node->type == EntryType::key)
        {
            return nullptr;
        }
        auto& dir = node->getDir();
        auto it = dir.tree.find(item, CacheNodeComparator());
        if(it == dir.tree.end())
        {
            return nullptr;
        }
        node = &*it;
    }
    return node;
}

void PHKVStorageImpl::store(boost::string_view keyPath, const ValueType& value)
{
    auto path = splitKeyPath(keyPath);
    if(path.empty())
    {
        throw std::runtime_error(fmt::format("Invalid path {}", keyPath));
    }
    auto key = path.back();
    path.pop_back();
    bool cached = false;
    {
        MountPointInfoPtr mount;
        uint32_t volumeOpSeq;
        {
            LockGuard guard(m_cacheMtx);
            auto node = findInCache(path);
            if(node)
            {
                cached = true;
                auto it = node->getDir().tree.find(key, CacheNodeComparator());
                if(it != node->getDir().tree.end())
                {
                    m_cachePool.touch(&*it);
                    std::tie(mount, volumeOpSeq) = getVolumeByIdAndAllocateOpSeq(it->volumeId);
                }
            }
        }
        if(mount)
        {
            executeOpInSequence(*mount, volumeOpSeq, [mount, keyPath, &value]() {
                mount->volume->store(keyPath, value);
            });
            return;
        }
    }
    FoundVolumes volumes = findVolumeByPath(keyPath);
    if(volumes.type() == typeid(VolumeNotFound))
    {
        throw std::runtime_error(fmt::format("No volumes were mount for path {}", keyPath));
    }
    if(cached)
    {
        MountPointInfoPtr mount;
        if(volumes.type() == typeid(VolumeId))
        {
            mount = boost::get<MountPointInfoPtr>(volumes);
        }
        else
        {
            mount = boost::get<std::vector<MountPointInfoPtr>>(volumes).front();
        }

    }
}

void PHKVStorageImpl::storeExpiring(boost::string_view keyPath, const ValueType& value, TimePoint expTime)
{

}

boost::optional<PHKVStorageImpl::ValueType> PHKVStorageImpl::lookup(boost::string_view keyPath)
{
    return {};
}

void PHKVStorageImpl::eraseKey(boost::string_view keyPath)
{

}

void PHKVStorageImpl::eraseDirRecursive(boost::string_view dirPath)
{

}

boost::optional<std::vector<PHKVStorageImpl::DirEntry>> PHKVStorageImpl::getDirEntries(boost::string_view dirPath)
{
    return {};
}

}

static PHKVStorage::UniquePtr create(const PHKVStorage::Options& options)
{
    return std::make_unique<PHKVStorageImpl>(options);
}

}
