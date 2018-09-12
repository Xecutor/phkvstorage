#pragma once

#include <string>

#include <boost/variant.hpp>
#include <boost/optional.hpp>

#include "IRandomAccessFile.hpp"
#include "FileSystem.hpp"
#include "SmallToMediumFileStorage.hpp"
#include "BigFileStorage.hpp"


namespace phkvs{

class StorageVolume{
public:
    using ValueType = boost::variant<uint8_t, uint16_t, uint32_t, uint64_t,
        float, double, std::string, std::vector<uint8_t>>;

    static std::unique_ptr<StorageVolume> open(FileSystem::UniqueFilePtr&& mainFile,
                                               FileSystem::UniqueFilePtr&& stmFile,
                                               FileSystem::UniqueFilePtr&& bigFile);
    static std::unique_ptr<StorageVolume> create(FileSystem::UniqueFilePtr&& mainFile,
                                                 FileSystem::UniqueFilePtr&& stmFile,
                                                 FileSystem::UniqueFilePtr&& bigFile);

    void store(const std::string& keyPath, const ValueType& value);

private:

    using OffsetType = IRandomAccessFile::OffsetType;

    //static constexpr size_t k_
    enum class EntryType{
        dir,
        key
    };

    struct KeyInfo{
        std::string value;
        OffsetType offset;
    };

    struct ValueInfo{
        ValueType value;
        OffsetType offset;
    };

    struct Entry{
        EntryType type;
        KeyInfo key;
        ValueInfo value;
    };

    FileSystem::UniqueFilePtr m_mainFile;
    std::unique_ptr<SmallToMediumFileStorage> m_stmStorage;
    std::unique_ptr<BigFileStorage> m_bigStorage;

    struct PrivateKey;
public:
    StorageVolume(PrivateKey&, FileSystem::UniqueFilePtr&& mainFile):m_mainFile(std::move(mainFile)){}
};

}
