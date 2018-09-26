#pragma once

#include <string>
#include <chrono>

#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <boost/utility/string_view.hpp>

#include "FileSystem.hpp"
#include "SmallToMediumFileStorage.hpp"
#include "BigFileStorage.hpp"


namespace phkvs{

class StorageVolume{
public:
    using ValueType = boost::variant<uint8_t, uint16_t, uint32_t, uint64_t,
        float, double, std::string, std::vector<uint8_t>>;

    using UniquePtr = std::unique_ptr<StorageVolume>;
    using TimePoint = std::chrono::system_clock::time_point;

    enum class EntryType {
        key,
        dir
    };

    struct DirEntry {
        EntryType type;
        std::string name;
    };

    static UniquePtr open(FileSystem::UniqueFilePtr&& mainFile,
                                               SmallToMediumFileStorage::UniquePtr&& stmFileStorage,
                                               BigFileStorage::UniquePtr&& bigFileStorage);
    static UniquePtr create(FileSystem::UniqueFilePtr&& mainFile,
                                                 SmallToMediumFileStorage::UniquePtr&& stmFileStorage,
                                                 BigFileStorage::UniquePtr&& bigFileStorage);

    virtual ~StorageVolume() = default;
    virtual void store(boost::string_view keyPath, const ValueType& value) = 0;
    virtual void storeExpiring(boost::string_view keyPath, const ValueType& value, TimePoint expTime) = 0;
    virtual boost::optional<ValueType> lookup(boost::string_view keyPath) = 0;
    virtual void eraseKey(boost::string_view keyPath) = 0;
    virtual void eraseDirRecursive(boost::string_view dirPath) = 0;

    virtual boost::optional<std::vector<DirEntry>> getDirEntries(boost::string_view dirPath) = 0;

    virtual void dump(const std::function<void(const std::string&)>& out) = 0;

    static void initFileLogger(const boost::filesystem::path& filePath, size_t maxSize, size_t maxFiles);
    static void initStdoutLogger();

};

}
