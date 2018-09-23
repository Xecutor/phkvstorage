#include "BigFileStorage.hpp"

#include "UIntArrayHexFormatter.hpp"
#include "FileOpsHelpers.hpp"
#include "FileVersion.hpp"
#include "FileMagic.hpp"


namespace phkvs {

namespace {
class BigFileStorageImpl : public BigFileStorage {
public:
    //actually private ctor
    explicit BigFileStorageImpl(FileSystem::UniqueFilePtr&& file) : m_file(std::move(file))
    {

    }

    OffsetType allocateAndWrite(boost::asio::const_buffer buf) override;
    void overwrite(OffsetType offset, boost::asio::const_buffer buf) override;
    void read(OffsetType offset, boost::asio::mutable_buffer buf) override;

    virtual void free(OffsetType offset) override;

    void openImpl();

    void createImpl();

private:
    static const FileMagic s_magic;
    static const FileVersion s_currentVersion;
    static constexpr size_t k_headerSize = FileMagic::binSize() + FileVersion::binSize() + sizeof(OffsetType);
    static constexpr size_t k_firstPagePointerOffset = FileMagic::binSize() + FileVersion::binSize();
    static constexpr size_t k_pageFullSize = 512;
    static constexpr size_t k_pageDataSize = k_pageFullSize - sizeof(OffsetType);

    OffsetType allocatePage(OffsetType& fileSize);

    static void throwIfOffsetIsInvalid(OffsetType offset, const char* funcName);

    OffsetType m_firstFreePage = 0;
    FileSystem::UniqueFilePtr m_file;

};

const FileMagic BigFileStorageImpl::s_magic{{'B', 'G', 'F', 'S'}};
const FileVersion BigFileStorageImpl::s_currentVersion{0x0001, 0x0000};

void BigFileStorageImpl::openImpl()
{
    auto fileSize = m_file->seekEnd();
    if(fileSize == 0 || (fileSize % k_pageFullSize) != 0)
    {
        throw std::runtime_error(
            fmt::format("Unexpected file size of {} for BigFileStorageImpl:{}",
                        m_file->getFilename().string(), fileSize));
    }
    std::array<uint8_t, k_headerSize> headerData{};
    auto buf = boost::asio::buffer(headerData);
    m_file->seek(0);
    m_file->read(buf);
    InputBinBuffer in(buf);

    FileMagic magic;
    magic.deserialize(in);
    if(magic != s_magic)
    {
        throw std::runtime_error(
            fmt::format("BigFileStorageImpl: invalid magic in file {}. Expected {}, but found {}",
                        m_file->getFilename().string(), s_magic, magic));
    }

    FileVersion version{0, 0};
    version.deserialize(in);
    if(version != s_currentVersion)
    {
        throw std::runtime_error(
            fmt::format("BigFileStorageImpl: invalid version of file {}. Expected {}, but found {}",
                        m_file->getFilename().string(), s_magic, magic));
    }
    m_firstFreePage = in.readU64();
}

void BigFileStorageImpl::createImpl()
{
    auto fileSize = m_file->seekEnd();
    if(fileSize != 0)
    {
        throw std::runtime_error(fmt::format("File {} must be empty for SmallToMediumFileStorage:{}",
                                             m_file->getFilename().string(), fileSize));
    }
    std::array<uint8_t, k_pageFullSize> headerData{};
    auto buf = boost::asio::buffer(headerData);
    OutputBinBuffer out(buf);
    s_magic.serialize(out);
    s_currentVersion.serialize(out);
    out.writeU64(0);
    m_file->write(buf);
}

BigFileStorageImpl::OffsetType BigFileStorageImpl::allocatePage(OffsetType& fileSize)
{
    OffsetType rv;
    if(m_firstFreePage)
    {
        rv = m_firstFreePage;
        readUIntAt(*m_file, m_firstFreePage, m_firstFreePage);
        writeUIntAt(*m_file, k_firstPagePointerOffset, m_firstFreePage);
    } else
    {
        if(!fileSize)
        {
            fileSize = m_file->seekEnd();
        } else
        {
            fileSize += k_pageFullSize;
        }
        rv = fileSize;
    }

    return rv;
}

BigFileStorageImpl::OffsetType BigFileStorageImpl::allocateAndWrite(boost::asio::const_buffer buf)
{
    OffsetType fileSize = 0;
    OffsetType rv = allocatePage(fileSize);
    OffsetType currentPageOffset = rv;
    while(buf.size())
    {
        OffsetType nextPageOffset = 0;
        size_t toWrite = k_pageDataSize;
        if(buf.size() > k_pageDataSize)
        {
            nextPageOffset = allocatePage(fileSize);
        } else
        {
            toWrite = buf.size();
        }
        std::array<uint8_t, k_pageFullSize> pageData{};
        OutputBinBuffer out(boost::asio::buffer(pageData));
        out.writeU64(nextPageOffset);
        out.writeBufAndAdvance(buf, toWrite);
        m_file->seek(currentPageOffset);
        m_file->write(boost::asio::buffer(pageData));
        currentPageOffset = nextPageOffset;
    }
    return rv;
}

void BigFileStorageImpl::overwrite(BigFileStorageImpl::OffsetType offset, boost::asio::const_buffer buf)
{
    throwIfOffsetIsInvalid(offset, "overwrite");

    OffsetType fileSize = 0;
    OffsetType nextPageOffset = 0;
    OffsetType currentPageOffset = offset;
    bool extraSpaceAllocated = false;
    while(buf.size())
    {
        if(!extraSpaceAllocated)
        {
            readUIntAt(*m_file, currentPageOffset, nextPageOffset);
        }
        size_t toWrite = k_pageDataSize;
        bool lastPage = buf.size() <= k_pageDataSize;
        if(lastPage)
        {
            toWrite = buf.size();
        } else
        {
            if(!nextPageOffset)
            {
                nextPageOffset = allocatePage(fileSize);
                extraSpaceAllocated = true;
            }
        }
        std::array<uint8_t, k_pageFullSize> pageData{};
        OutputBinBuffer out(boost::asio::buffer(pageData));
        out.writeU64(lastPage ? 0 : nextPageOffset);
        out.writeBufAndAdvance(buf, toWrite);
        m_file->seek(currentPageOffset);
        m_file->write(boost::asio::buffer(pageData));
        currentPageOffset = nextPageOffset;
    }
    if(!extraSpaceAllocated && nextPageOffset)
    {
        free(nextPageOffset);
    }
}

void BigFileStorageImpl::read(OffsetType offset, boost::asio::mutable_buffer buf)
{
    throwIfOffsetIsInvalid(offset, "read");
    OffsetType currentPageOffset = offset;
    while(buf.size())
    {
        std::array<uint8_t, k_pageFullSize> pageData{};
        auto pageBuf = boost::asio::buffer(pageData);
        m_file->seek(currentPageOffset);
        m_file->read(pageBuf);
        InputBinBuffer in(pageBuf);
        OffsetType nextPageOffset = in.readU64();
        //min takes args as const ref. This forces
        //k_pageDataSize to have an address in C++ before 17.
        const size_t pageDataSize = k_pageDataSize;
        size_t toRead = std::min(pageDataSize, buf.size());
        in.readBufAndAdvance(buf, toRead);
        currentPageOffset = nextPageOffset;
    }
}

void BigFileStorageImpl::free(OffsetType offset)
{
    throwIfOffsetIsInvalid(offset, "free");
    if(m_firstFreePage)
    {
        OffsetType lastPageOffset = offset;
        OffsetType nextPageOffset;
        do
        {
            readUIntAt(*m_file, lastPageOffset, nextPageOffset);
            if(nextPageOffset)
            {
                lastPageOffset = nextPageOffset;
            }
        } while(nextPageOffset);
        writeUIntAt(*m_file, lastPageOffset, m_firstFreePage);
    }
    m_firstFreePage = offset;
    writeUIntAt(*m_file, k_firstPagePointerOffset, m_firstFreePage);
}

void BigFileStorageImpl::throwIfOffsetIsInvalid(OffsetType offset, const char* funcName)
{
    if((offset % k_pageFullSize) != 0)
    {
        throw std::runtime_error(fmt::format("BigFileStorage::{}:Invalid offset {}", funcName, offset));
    }
}

}

BigFileStorage::UniquePtr BigFileStorage::open(FileSystem::UniqueFilePtr&& file)
{
    auto rv = std::make_unique<BigFileStorageImpl>(std::move(file));
    rv->openImpl();
    return rv;
}

BigFileStorage::UniquePtr BigFileStorage::create(FileSystem::UniqueFilePtr&& file)
{
    auto rv = std::make_unique<BigFileStorageImpl>(std::move(file));
    rv->createImpl();
    return rv;
}

}