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

    using ValueType = boost::variant<uint8_t, uint16_t, uint32_t, uint64_t,
            float, double, std::string, std::vector<uint8_t>>;
    using TimePoint = std::chrono::system_clock::time_point;

    enum class EntryType {
        key,
        dir
    };

    struct DirEntry {
        EntryType type;
        std::string name;
    };

    using VolumeId = uint32_t;

    virtual VolumeId createAndMountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                                      boost::string_view mountPointPath) = 0;

    virtual VolumeId mountVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                             boost::string_view mountPointPath) = 0;

    virtual void unmountVolume(VolumeId volumeId) = 0;

    virtual void store(boost::string_view keyPath, const ValueType& value) = 0;
    virtual void storeExpiring(boost::string_view keyPath, const ValueType& value, TimePoint expTime) = 0;

    virtual boost::optional<ValueType> lookup(boost::string_view keyPath) = 0;

    virtual void eraseKey(boost::string_view keyPath) = 0;
    virtual void eraseDirRecursive(boost::string_view dirPath) = 0;

    virtual boost::optional<std::vector<DirEntry>> getDirEntries(boost::string_view dirPath) = 0;

    virtual ~PHKVStorage() = default;
};

}
