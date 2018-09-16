#pragma once

#include "FileSystem.hpp"
#include "FileVersion.hpp"
#include "FileMagic.hpp"

namespace phkvs{

class BigFileStorage{
public:

    using OffsetType = IRandomAccessFile::OffsetType;

    static std::unique_ptr<BigFileStorage> open(FileSystem::UniqueFilePtr&& file);
    static std::unique_ptr<BigFileStorage> create(FileSystem::UniqueFilePtr&& file);

    OffsetType allocateAndWrite(boost::asio::const_buffer buf);
    void overwrite(OffsetType offset, boost::asio::const_buffer buf);
    void read(OffsetType offset, boost::asio::mutable_buffer buf);

    void free(OffsetType offset);

private:
    static const FileMagic s_magic;
    static const FileVersion s_currentVersion;
    static constexpr size_t k_headerSize = FileMagic::binSize() + FileVersion::binSize() + sizeof(OffsetType);
    static constexpr size_t k_firstPagePointerOffset = FileMagic::binSize() + FileVersion::binSize();
    static constexpr size_t k_pageFullSize = 512;
    static constexpr size_t k_pageDataSize = k_pageFullSize - sizeof(OffsetType);



    void openImpl();
    void createImpl();

    OffsetType allocatePage(OffsetType& fileSize);

    static void throwIfOffsetIsInvalid(OffsetType offset, const char* funcName);

    OffsetType m_firstFreePage = 0;
    FileSystem::UniqueFilePtr m_file;

    struct PrivateKey;

public:
    //actually private ctor
    explicit BigFileStorage(PrivateKey&,FileSystem::UniqueFilePtr&& file):m_file(std::move(file))
    {

    }

};

}