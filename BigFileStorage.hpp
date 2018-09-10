#pragma once

#include "FileSystem.hpp"
#include "FileVersion.hpp"

namespace phkvs{

class BigFileStorage{
public:

    using OffsetType = IRandomAccessFile::OffsetType;

    explicit BigFileStorage(FileSystem::UniqueFilePtr&& file):m_file(std::move(file))
    {

    }
    void open();
    void create();

    OffsetType allocateAndWrite(boost::asio::const_buffer buf);
    void overwrite(OffsetType offset, boost::asio::const_buffer buf);
    void read(OffsetType offset, boost::asio::mutable_buffer buf);

    void free(OffsetType offset);

private:
    using MagicType = std::array<uint8_t,4>;
    static constexpr MagicType s_magic {'B', 'G', 'F', 'S'};
    static constexpr FileVersion s_currentVersion { 0x0001, 0x0000};
    static constexpr size_t k_headerSize = sizeof(s_magic) + sizeof(s_currentVersion) + sizeof(OffsetType);
    static constexpr size_t k_firstPagePointerOffset = sizeof(s_magic) + sizeof(s_currentVersion);
    static constexpr size_t k_pageFullSize = 512;
    static constexpr size_t k_pageDataSize = k_pageFullSize - sizeof(OffsetType);

    OffsetType allocatePage(OffsetType& fileSize);

    static void throwIfOffsetIsInvalid(OffsetType offset, const char* funcName);

    FileSystem::UniqueFilePtr m_file;
    OffsetType m_firstFreePage = 0;
};

}