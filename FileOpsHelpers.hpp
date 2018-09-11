#pragma once

#include <type_traits>

#include "IRandomAccessFile.hpp"
#include "InputBinBuffer.hpp"
#include "OutputBinBuffer.hpp"


namespace phkvs {

namespace details {

inline void putUIntToBuf(OutputBinBuffer& out, uint8_t value)
{
    out.writeU8(value);
}

inline void putUIntToBuf(OutputBinBuffer& out, uint16_t value)
{
    out.writeU16(value);
}

inline void putUIntToBuf(OutputBinBuffer& out, uint32_t value)
{
    out.writeU32(value);
}

inline void putUIntToBuf(OutputBinBuffer& out, uint64_t value)
{
    out.writeU64(value);
}

inline void getUIntFromBuf(InputBinBuffer& in, uint8_t& value)
{
    value = in.readU8();
}

inline void getUIntFromBuf(InputBinBuffer& in, uint16_t& value)
{
    value = in.readU16();
}

inline void getUIntFromBuf(InputBinBuffer& in, uint32_t& value)
{
    value = in.readU32();
}

inline void getUIntFromBuf(InputBinBuffer& in, uint64_t& value)
{
    value = in.readU64();
}

}

template<class T>
inline void writeUIntAt(IRandomAccessFile& file, IRandomAccessFile::OffsetType offset, T value)
{
    static_assert(std::is_integral_v<T>, "writeUIntAt is for integral types only");
    static_assert(sizeof(T) <= 8, "Unsupported int type size");
    std::array<uint8_t, sizeof(T)> data;
    OutputBinBuffer outBuf(boost::asio::buffer(data));

    using namespace phkvs::details;

    putUIntToBuf(outBuf, value);

    file.seek(offset);
    file.write(boost::asio::buffer(data));
}

template<class T>
void readUIntAt(IRandomAccessFile& file, IRandomAccessFile::OffsetType offset, T& value)
{
    static_assert(std::is_integral_v<T>, "readUIntAt is for integral types only");
    static_assert(sizeof(T) <= 8, "Unsupported int type size");
    std::array<uint8_t, sizeof(T)> data;
    file.seek(offset);
    auto buf = boost::asio::buffer(data);
    file.read(buf);
    InputBinBuffer inBuf(buf);
    using namespace phkvs::details;
    getUIntFromBuf(inBuf, value);
}

}