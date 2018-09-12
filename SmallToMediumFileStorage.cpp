#include "SmallToMediumFileStorage.hpp"

#include "UIntArrayHexFormatter.hpp"
#include "FileOpsHelpers.hpp"

namespace phkvs {

const FileMagic SmallToMediumFileStorage::s_magic{'S', 'M', 'F', 'S'};
const FileVersion SmallToMediumFileStorage::s_currentVersion{0x0001, 0x0000};

struct SmallToMediumFileStorage::PrivateKey{};

std::unique_ptr<SmallToMediumFileStorage> SmallToMediumFileStorage::open(FileSystem::UniqueFilePtr&& file)
{
    PrivateKey pkey;
    auto rv = std::make_unique<SmallToMediumFileStorage>(pkey, std::move(file));
    rv->openImpl();
    return rv;
}

std::unique_ptr<SmallToMediumFileStorage> SmallToMediumFileStorage::create(FileSystem::UniqueFilePtr&& file)
{
    PrivateKey pkey;
    auto rv = std::make_unique<SmallToMediumFileStorage>(pkey, std::move(file));
    rv->createImpl();
    return rv;
}


void SmallToMediumFileStorage::openImpl()
{
    auto fileSize = m_file->seekEnd();
    if(fileSize < k_headerSize)
    {
        throw std::runtime_error(
            fmt::format("Unexpected file size of {} for SmallToMediumFileStorage:{}",
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
            fmt::format("SmallToMediumFileStorage: invalid magic in file {}. Expected {}, but found {}",
                        m_file->getFilename().string(), s_magic, magic));
    }

    FileVersion version{0, 0};
    version.deserialize(in);
    if(version != s_currentVersion)
    {
        throw std::runtime_error(
            fmt::format("SmallToMediumFileStorage: invalid version of file {}. Expected {}, but found {}",
                        m_file->getFilename().string(), s_currentVersion, version));
    }
    for(size_t i = 0; i < k_slotsCount; ++i)
    {
        m_freeSlotsListOffset[i] = in.readU64();
    }
}

void SmallToMediumFileStorage::createImpl()
{
    auto fileSize = m_file->seekEnd();
    if(fileSize != 0)
    {
        throw std::runtime_error(fmt::format("File {} must be empty for SmallToMediumFileStorage:{}",
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

SmallToMediumFileStorage::OffsetType SmallToMediumFileStorage::allocateAndWrite(boost::asio::const_buffer buf)
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
    } else
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

SmallToMediumFileStorage::OffsetType
SmallToMediumFileStorage::overwrite(OffsetType offset, size_t oldSize, boost::asio::const_buffer buf)
{
    size_t oldIndex = sizeToSlotIndex(oldSize);
    size_t newIndex = sizeToSlotIndex(buf.size());
    if(oldIndex == newIndex)
    {
        m_file->seek(offset);
        m_file->write(buf);
    } else
    {
        freeSlot(offset, oldSize);
        offset = allocateAndWrite(buf);
    }
    return offset;
}

void SmallToMediumFileStorage::read(OffsetType offset, boost::asio::mutable_buffer buf)
{
    m_file->seek(offset);
    m_file->read(buf);
}

void SmallToMediumFileStorage::freeSlot(OffsetType offset, size_t size)
{
    size_t index = sizeToSlotIndex(size);
    writeUIntAt(*m_file, offset, m_freeSlotsListOffset[index]);
    m_freeSlotsListOffset[index] = offset;
    writeUIntAt(*m_file, offsetForFreeSlotByIndex(index), offset);
}

}