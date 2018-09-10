#pragma once

#include <stdexcept>
#include <fmt/format.h>
#include <boost/asio/buffer.hpp>
#include <boost/endian/buffers.hpp>

namespace phkvs {

class InputBinBuffer {
public:
    InputBinBuffer(boost::asio::const_buffer buf) : m_buf(buf)
    {
    }

    uint8_t readU8()
    {
        return read<uint8_t>();
    }

    uint16_t readU16()
    {
        return read<boost::endian::little_uint16_buf_t>().value();
    }

    uint32_t readU32()
    {
        return read<boost::endian::little_uint32_buf_t>().value();
    }

    uint64_t readU64()
    {
        return read<boost::endian::little_uint64_buf_t>().value();
    }

    template<size_t N>
    void readArray(std::array<uint8_t, N>& array)
    {
        checkRemainingSpaceAndThrow(N);
        memcpy(array.data(), m_buf.data(), N);
        m_buf += N;
    }

    void readBufAndAdvance(boost::asio::mutable_buffer& buf, size_t amount)
    {
        checkRemainingSpaceAndThrow(amount);
        if(amount > buf.size())
        {
            throw std::out_of_range(
                fmt::format("Attempt to read {} bytes into buffer with {} bytes",
                            amount, buf.size()));
        }
        memcpy(buf.data(), m_buf.data(), amount);
        m_buf += amount;
        buf += amount;
    }

    float readFloat()
    {
        static_assert(sizeof(uint32_t) == sizeof(float), "float is not 32 bit");
        uint32_t rv = readU32();
        //Questionable solution, but REALLY portable way of float serialization
        //is out of scope of this project. Let's assume this is portable
        //across platforms with ISO/IEC/IEEE 60559:2011 float/double.
        return reinterpret_cast<float&>(rv);
    }

    double readDouble()
    {
        static_assert(sizeof(uint64_t) == sizeof(double), "double is not 64 bit");
        uint64_t rv = readU64();
        //same as float
        return reinterpret_cast<double&>(rv);
    }

    size_t remainingSpace() const
    {
        return m_buf.size();
    }

private:
    boost::asio::const_buffer m_buf;

    void checkRemainingSpaceAndThrow(size_t amount)
    {
        if(amount > remainingSpace())
        {
            throw std::out_of_range(
                fmt::format("Attempt to read {} bytes when {} bytes remaining in the input buffer",
                            amount, m_buf.size()));
        }
    }

    template<class T>
    const T& read()
    {
        checkRemainingSpaceAndThrow(sizeof(T));
        const T* rv = reinterpret_cast<const T*>(m_buf.data());
        m_buf += sizeof(T);
        return *rv;
    }
};

}
