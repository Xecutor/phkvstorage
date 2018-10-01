#pragma once

#include "FileSystem.hpp"
#include "SmallToMediumFileStorage.hpp"
#include "BigFileStorage.hpp"
#include "PHKVStorage.hpp"


namespace phkvs{

class StorageVolume{
public:

    using UniquePtr = std::unique_ptr<StorageVolume>;

    using ValueType = PHKVStorage::ValueType;
    using TimePoint = PHKVStorage::TimePoint;
    using TimePointOpt = PHKVStorage::TimePointOpt;
    using DirEntry = PHKVStorage::DirEntry;

    static UniquePtr open(FileSystem::UniqueFilePtr&& mainFile,
                                               SmallToMediumFileStorage::UniquePtr&& stmFileStorage,
                                               BigFileStorage::UniquePtr&& bigFileStorage);
    static UniquePtr create(FileSystem::UniqueFilePtr&& mainFile,
                                                 SmallToMediumFileStorage::UniquePtr&& stmFileStorage,
                                                 BigFileStorage::UniquePtr&& bigFileStorage);

    static void initFileLogger(const boost::filesystem::path& filePath, size_t maxSize, size_t maxFiles);
    static void initStdoutLogger();

    virtual void store(boost::string_view keyPath, const ValueType& value, TimePointOpt expTime = {}) = 0;

    virtual boost::optional<ValueType> lookup(boost::string_view keyPath) = 0;

    virtual void eraseKey(boost::string_view keyPath) = 0;
    virtual void eraseDirRecursive(boost::string_view dirPath) = 0;

    virtual boost::optional<std::vector<DirEntry>> getDirEntries(boost::string_view dirPath) = 0;

    virtual void dump(const std::function<void(const std::string&)>& out) = 0;

    virtual ~StorageVolume() = default;

};

}
