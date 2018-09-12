#pragma once

#include <array>
#include <stdexcept>

#include "FileSystem.hpp"
#include "FileVersion.hpp"
#include "FileMagic.hpp"

#include <fmt/format.h>


namespace phkvs{

class SmallToMediumFileStorage{
public:

    using OffsetType = IRandomAccessFile::OffsetType;

    static std::unique_ptr<SmallToMediumFileStorage> open(FileSystem::UniqueFilePtr&& file);
    static std::unique_ptr<SmallToMediumFileStorage> create(FileSystem::UniqueFilePtr&& file);

    OffsetType allocateAndWrite(boost::asio::const_buffer buf);
    OffsetType overwrite(OffsetType offset, size_t oldSize, boost::asio::const_buffer buf);
    void read(OffsetType offset, boost::asio::mutable_buffer buf);

    void freeSlot(OffsetType offset, size_t size);

    static constexpr size_t slotSizeIncrement()
    {
        return k_slotSizeIncrement;
    }

    static constexpr size_t maxDataSize()
    {
        return (k_slotsCount + 1) * k_slotSizeIncrement;
    }

private:
    static constexpr size_t k_offsetSize = 8;
    static constexpr size_t k_slotSizeIncrement = 8;
    //for object from 9 (two slots) to 256 (32 slots)
    static constexpr size_t k_slotsCount = 31;

    static const FileMagic s_magic;
    static const FileVersion s_currentVersion;
    static constexpr size_t k_headerSize = FileMagic::binSize() + FileVersion::binSize() + k_slotsCount * k_offsetSize;

    void openImpl();
    void createImpl();

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

    struct PrivateKey;

public:
    SmallToMediumFileStorage(PrivateKey&, FileSystem::UniqueFilePtr&& file):m_file(std::move(file))
    {}
};

}