#include "PHKVStorage.hpp"

#include <mutex>
#include <map>
#include <stdexcept>

#include <fmt/format.h>

#include "StorageVolume.hpp"
#include "StringViewFormatter.hpp"

namespace phkvs {

namespace {

std::string toString(boost::string_view sv)
{
    return {sv.data(), sv.length()};
}

class PHKVStorageImpl : public PHKVStorage {
public:
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
    using LockGuard = std::lock_guard<std::mutex>;
    std::mutex m_mountInfoMtx;

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

    using MountPointsMap = std::multimap<std::string, MountPointInfo>;
    MountPointsMap m_mountPoints;
    VolumeId m_lastVolumeId{0};
    std::map<VolumeId, MountPointsMap::iterator> m_volumeIdMap;
};

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
    auto stmPath = makeMainFileFullPath(volumePath, volumeNameStr);
    auto bigPath = makeMainFileFullPath(volumePath, volumeNameStr);
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

    auto it = m_mountPoints.emplace(std::piecewise_construct, std::make_tuple(toString(mountPointPath)),
                                    std::make_tuple());
    auto& info = it->second;
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
    return 0;
}

void PHKVStorageImpl::unmountVolume(VolumeId volumeId)
{

}

std::vector<PHKVStorageImpl::VolumeInfo> PHKVStorageImpl::getMountVolumesInfo() const
{
    return {};
}

void PHKVStorageImpl::store(boost::string_view keyPath, const ValueType& value)
{

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

}
