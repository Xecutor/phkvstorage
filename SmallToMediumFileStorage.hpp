#pragma once

#include "FileSystem.hpp"

namespace phkvs{

class SmallToMediumFileStorage{
public:

    using OffsetType = IRandomAccessFile::OffsetType;
    using UniquePtr = std::unique_ptr<SmallToMediumFileStorage>;

    static UniquePtr open(FileSystem::UniqueFilePtr&& file);
    static UniquePtr create(FileSystem::UniqueFilePtr&& file);

    virtual ~SmallToMediumFileStorage() = default;

    virtual OffsetType allocateAndWrite(boost::asio::const_buffer buf) = 0;
    virtual OffsetType overwrite(OffsetType offset, size_t oldSize, boost::asio::const_buffer buf) = 0;
    virtual void read(OffsetType offset, boost::asio::mutable_buffer buf) = 0;

    virtual void freeSlot(OffsetType offset, size_t size) = 0;

    static constexpr size_t slotSizeIncrement()
    {
        return k_slotSizeIncrement;
    }

    static constexpr size_t maxDataSize()
    {
        return (k_slotsCount + 1) * k_slotSizeIncrement;
    }

protected:
    static constexpr size_t k_slotSizeIncrement = 8;
    //for object from 9 (two slots) to 256 (32 slots)
    static constexpr size_t k_slotsCount = 31;

};

}