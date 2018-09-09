#pragma once

#include <stdio.h>
#include <boost/asio/buffer.hpp>
#include <boost/filesystem/path.hpp>

namespace phkvs{

class IRandomAccessFile{
public:
    using OffsetType = uint64_t;
    virtual ~IRandomAccessFile() = default;
    virtual void read(boost::asio::mutable_buffer& buf) = 0;
    virtual void write(boost::asio::const_buffer buf) = 0;
    //Seek to specified absolute offset
    virtual void seek(OffsetType offset) = 0;
    //Seek to the end of the file and return file size
    virtual OffsetType seekEnd() = 0;

    virtual const boost::filesystem::path& getFilename()const = 0;
};

}
