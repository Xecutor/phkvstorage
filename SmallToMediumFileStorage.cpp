#include "SmallToMediumFileStorage.hpp"

#include <array>
#include <stdexcept>

#include <fmt/format.h>

#include "FileVersion.hpp"
#include "FileMagic.hpp"
#include "UIntArrayHexFormatter.hpp"
#include "FileOpsHelpers.hpp"

namespace phkvs {
namespace {

class SmallToMediumFileStorageImpl : public SmallToMediumFileStorage {
public:

    SmallToMediumFileStorageImpl(FileSystem::UniqueFilePtr&& file) : m_file(std::move(file))
    {
    }

    OffsetType allocateAndWrite(boost::asio::const_buffer buf) override;

    OffsetType overwrite(OffsetType offset, size_t oldSize, boost::asio::const_buffer buf) override;

    void read(OffsetType offset, boost::asio::mutable_buffer buf) override;

    void freeSlot(OffsetType offset, size_t size) override;

    void openImpl();

    void createImpl();

private:
    static constexpr size_t k_offsetSize = 8;
    static const FileMagic s_magic;
    static const FileVersion s_currentVersion;
    static constexpr size_t k_headerSize = FileMagic::binSize() + FileVersion::binSize() + k_slotsCount * k_offsetSize;

    static size_t sizeToSlotIndex(size_t size)
    {
        if(size < 9)
        {
            return 0;
        }
        //For sizes 9..16 index should be 0
        //So index = size/k_slotSizeIncrement rounded up and then minus 2
        //Instead of -2 and then +1 if size is not multiple of k_slotSizeIncrement
        //we will make -1 and then -1 again if size is multiple of k_slotSizeIncrement,
        //which is, from statistics point of view less frequent condition.
        size_t rv = (size / k_slotSizeIncrement) - 1;
        if(!(size % k_slotSizeIncrement))
        {
            --rv;
        }
        if(rv >= k_slotsCount)
        {
            throw std::runtime_error(fmt::format("Size {} is too big for SmallToMediumFileStorage", size));
        }
        return rv;
    }

    static size_t maxSlotSizeForIndex(size_t index)
    {
        return (index + 2) * k_slotSizeIncrement;
    }

    static OffsetType offsetForFreeSlotByIndex(size_t index)
    {
        return FileMagic::binSize() + FileVersion::binSize() + index * k_offsetSize;
    }

    FileSystem::UniqueFilePtr m_file;
    std::array<uint64_t, k_slotsCount> m_freeSlotsListOffset{};

};

const FileMagic SmallToMediumFileStorageImpl::s_magic{{'S', 'M', 'F', 'S'}};
const FileVersion SmallToMediumFileStorageImpl::s_currentVersion{0x0001, 0x0000};


void SmallToMediumFileStorageImpl::openImpl()
{
    auto fileSize = m_file->seekEnd();
    if(fileSize < k_headerSize)
    {
        throw std::runtime_error(
            fmt::format("Unexpected file size of {} for SmallToMediumFileStorageImpl:{}",
                        m_file->getFilename().string(), fileSize));
    }
    m_file->seek(0);

    std::array<uint8_t, k_headerSize> headerData{};
    auto buf = boost::asio::buffer(headerData);

    m_file->read(buf);

    InputBinBuffer in(buf);

    FileMagic magic;

    magic.deserialize(in);
    if(magic != s_magic)
    {
        throw std::runtime_error(
            fmt::format("SmallToMediumFileStorageImpl: invalid magic in file {}. Expected {}, but found {}",
                        m_file->getFilename().string(), s_magic, magic));
    }

    FileVersion version{0, 0};
    version.deserialize(in);
    if(version != s_currentVersion)
    {
        throw std::runtime_error(
            fmt::format("SmallToMediumFileStorageImpl: invalid version of file {}. Expected {}, but found {}",
                        m_file->getFilename().string(), s_currentVersion, version));
    }
    for(size_t i = 0; i < k_slotsCount; ++i)
    {
        m_freeSlotsListOffset[i] = in.readU64();
    }
}

void SmallToMediumFileStorageImpl::createImpl()
{
    auto fileSize = m_file->seekEnd();
    if(fileSize != 0)
    {
        throw std::runtime_error(fmt::format("File {} must be empty for SmallToMediumFileStorageImpl:{}",
                                             m_file->getFilename().string(), fileSize));
    }
    std::array<uint8_t, k_headerSize> headerData{};
    auto buf = boost::asio::buffer(headerData);
    OutputBinBuffer out(buf);
    s_magic.serialize(out);
    s_currentVersion.serialize(out);
    for(size_t i = 0; i < k_slotsCount; ++i)
    {
        out.writeU64(0);
    }
    m_file->write(buf);
}

SmallToMediumFileStorageImpl::OffsetType SmallToMediumFileStorageImpl::allocateAndWrite(boost::asio::const_buffer buf)
{
    size_t index = sizeToSlotIndex(buf.size());
    OffsetType rv;
    if(m_freeSlotsListOffset[index] != 0)
    {
        rv = m_freeSlotsListOffset[index];
        OffsetType next;
        readUIntAt(*m_file, rv, next);
        writeUIntAt(*m_file, offsetForFreeSlotByIndex(index), next);
        m_freeSlotsListOffset[index] = next;
    }
    else
    {
        rv = m_file->seekEnd();
    }
    m_file->seek(rv);
    m_file->write(buf);

    size_t paddingSize = maxSlotSizeForIndex(index) - buf.size();
    if(paddingSize != 0)
    {
        uint8_t padding[256] = {0,};
        m_file->write(boost::asio::buffer(padding, paddingSize));
    }

    return rv;
}

SmallToMediumFileStorageImpl::OffsetType
SmallToMediumFileStorageImpl::overwrite(OffsetType offset, size_t oldSize, boost::asio::const_buffer buf)
{
    size_t oldIndex = sizeToSlotIndex(oldSize);
    size_t newIndex = sizeToSlotIndex(buf.size());
    if(oldIndex == newIndex)
    {
        m_file->seek(offset);
        m_file->write(buf);
    }
    else
    {
        freeSlot(offset, oldSize);
        offset = allocateAndWrite(buf);
    }
    return offset;
}

void SmallToMediumFileStorageImpl::read(OffsetType offset, boost::asio::mutable_buffer buf)
{
    m_file->seek(offset);
    m_file->read(buf);
}

void SmallToMediumFileStorageImpl::freeSlot(OffsetType offset, size_t size)
{
    size_t index = sizeToSlotIndex(size);
    writeUIntAt(*m_file, offset, m_freeSlotsListOffset[index]);
    m_freeSlotsListOffset[index] = offset;
    writeUIntAt(*m_file, offsetForFreeSlotByIndex(index), offset);
}

}

SmallToMediumFileStorage::UniquePtr SmallToMediumFileStorage::open(FileSystem::UniqueFilePtr&& file)
{
    auto rv = std::make_unique<SmallToMediumFileStorageImpl>(std::move(file));
    rv->openImpl();
    return rv;
}

SmallToMediumFileStorage::UniquePtr SmallToMediumFileStorage::create(FileSystem::UniqueFilePtr&& file)
{
    auto rv = std::make_unique<SmallToMediumFileStorageImpl>(std::move(file));
    rv->createImpl();
    return rv;
}

}