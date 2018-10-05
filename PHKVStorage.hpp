#pragma once

#include <string>
#include <chrono>

#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <boost/utility/string_view.hpp>

#include <boost/filesystem/path.hpp>

namespace phkvs{

class PHKVStorage{
public:

    struct Options{
        size_t cachePoolSize{16 * 1024};
    };

    using ValueType = boost::variant<uint8_t, uint16_t, uint32_t, uint64_t,
            float, double, std::string, std::vector<uint8_t>>;
    using TimePoint = std::chrono::system_clock::time_point;
    using TimePointOpt = boost::optional<std::chrono::system_clock::time_point>;

    using UniquePtr = std::unique_ptr<PHKVStorage>;

    enum class EntryType {
        key,
        dir
    };

    struct DirEntry {
        EntryType type;
        std::string name;
    };

    using VolumeId = uint32_t;

    struct VolumeInfo{
        boost::filesystem::path volumePath;
        std::string volumeName;
        std::string mountPointPath;
        VolumeId volumeId;
    };

    static UniquePtr create(const Options& options);
    static void deleteVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName);

    virtual VolumeId createAndMountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                                      boost::string_view mountPointPath) = 0;
    virtual VolumeId mountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                             boost::string_view mountPointPath) = 0;
    virtual void unmountVolume(VolumeId volumeId) = 0;
    virtual std::vector<VolumeInfo> getMountVolumesInfo() const = 0;

    virtual void store(boost::string_view keyPath, const ValueType& value, TimePointOpt expTime = {}) = 0;

    virtual boost::optional<ValueType> lookup(boost::string_view keyPath) = 0;

    virtual void eraseKey(boost::string_view keyPath) = 0;
    virtual void eraseDirRecursive(boost::string_view dirPath) = 0;

    virtual boost::optional<std::vector<DirEntry>> getDirEntries(boost::string_view dirPath) = 0;

    virtual ~PHKVStorage() = default;
};

}
