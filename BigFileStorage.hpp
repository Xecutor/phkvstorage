#pragma once

#include "FileSystem.hpp"

namespace phkvs{

class BigFileStorage{
public:

    using OffsetType = IRandomAccessFile::OffsetType;
    using UniquePtr = std::unique_ptr<BigFileStorage>;

    static UniquePtr open(FileSystem::UniqueFilePtr&& file);
    static UniquePtr create(FileSystem::UniqueFilePtr&& file);

    virtual OffsetType allocateAndWrite(boost::asio::const_buffer buf) = 0;
    virtual void overwrite(OffsetType offset, boost::asio::const_buffer buf) = 0;
    virtual void read(OffsetType offset, boost::asio::mutable_buffer buf) = 0;
    virtual void free(OffsetType offset) = 0;

    virtual ~BigFileStorage() = default;

};

}
