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
    if(prefix.length()>path.length())
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
        StorageVolume::UniquePtr volume;
        std::mutex volumeMtx;
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

    struct VolumeNotFound{};
    using FoundVolumes = boost::variant<VolumeNotFound, MountPointInfoPtr, std::vector<MountPointInfoPtr>>;

    FoundVolumes findVolumeByPath(boost::string_view path);

    std::atomic_uint_fast32_t m_cacheSeq{0};

    struct CacheTreeNode : boost::intrusive::set_base_hook<> {
        using CacheTree = boost::intrusive::rbtree<CacheTreeNode>;
        struct Dir {
            CacheTree tree;
        };

        EntryType type;
        bool overlapingDir;
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

        friend bool operator<(const CacheTreeNode& l, const CacheTreeNode& r)
        {
            return l.name < r.name;
        }

        friend bool operator<(const CacheTreeNode& l, boost::string_view rname)
        {
            return l.name < rname;
        }

        friend bool operator<(boost::string_view lname, const CacheTreeNode& r)
        {
            return lname < r.name;
        }

    };

    mutable std::mutex m_cacheMtx;
    using CachePoolType = LRUPriorityCachePool<CacheTreeNode, 2>;
    CachePoolType m_cachePool;
    CacheTreeNode m_cacheRoot;

    CacheTreeNode* findInCache(const std::vector<boost::string_view>& path);
    void cacheNodeReuseNotify(CachePoolType::PoolNode* node);
};

PHKVStorageImpl::PHKVStorageImpl(const Options& options):
    m_cachePool(options.cachePoolSize, std::bind(&PHKVStorageImpl::cacheNodeReuseNotify, this, std::placeholders::_1))
{
}

void PHKVStorageImpl::cacheNodeReuseNotify(CachePoolType::PoolNode* node)
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
    for(;it!=end;++it)
    {
        rv.push_back(it->second);
    }
    return {rv};
}

PHKVStorageImpl::CacheTreeNode* PHKVStorageImpl::findInCache(const std::vector<boost::string_view>& path)
{
    CacheTreeNode* node = &m_cacheRoot;
    for(auto& item : path)
    {
        if(node->type==EntryType::key)
        {
            return nullptr;
        }
        //auto it = node->getDir().tree;
    }
    return nullptr;
}

void PHKVStorageImpl::store(boost::string_view keyPath, const ValueType& value)
{
    auto path = splitKeyPath(keyPath);

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
