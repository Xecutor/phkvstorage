#pragma once

#include <stdexcept>
#include <fmt/format.h>
#include <boost/asio/buffer.hpp>
#include <boost/endian/buffers.hpp>

namespace phkvs{

class OutputBinBuffer {
public:
    OutputBinBuffer(boost::asio::mutable_buffer buf):m_buf(buf){}

    void writeU8(uint8_t value)
    {
        write<uint8_t>() = value;
    }

    void writeU16(uint16_t value)
    {
        write<boost::endian::little_uint16_buf_t>() = value;
    }

    void writeU32(uint32_t value)
    {
        write<boost::endian::little_uint32_buf_t>() = value;
    }

    void writeU64(uint64_t value)
    {
        write<boost::endian::little_uint64_buf_t>() = value;
    }

    void writeFloat(float value)
    {
        static_assert(sizeof(uint32_t) == sizeof(float), "float is not 32 bit");
        write<boost::endian::little_uint32_buf_t>() = reinterpret_cast<uint32_t&>(value);
    }

    void writeDouble(double value)
    {
        static_assert(sizeof(uint64_t) == sizeof(double), "double is not 64 bit");
        write<boost::endian::little_uint64_buf_t>() = reinterpret_cast<uint64_t&>(value);
    }

    template <size_t N>
    void writeArray(const std::array<uint8_t, N>& array)
    {
        checkRemainingSpaceAndThrow(N);
        memcpy(m_buf.data(), array.data(), N);
        m_buf += N;
    }

    void writeBufAndAdvance(boost::asio::const_buffer& buf, size_t amount)
    {
        checkRemainingSpaceAndThrow(amount);
        if(amount>buf.size())
        {
            throw std::out_of_range(
                fmt::format("Attempt to write {} bytes from buffer with {} bytes.",
                            amount, buf.size()));
        }
        memcpy(m_buf.data(), buf.data(), amount);
        m_buf += amount;
        buf += amount;
    }

    void fill(size_t amount, uint8_t value = 0)
    {
        checkRemainingSpaceAndThrow(amount);
        memset(m_buf.data(), 0, amount);
        m_buf += amount;
    }

    size_t remainingSpace()const
    {
        return m_buf.size();
    }

private:
    boost::asio::mutable_buffer m_buf;

    void checkRemainingSpaceAndThrow(size_t amount)
    {
        if(amount > remainingSpace())
        throw std::out_of_range(
            fmt::format("Attempt to write {} bytes when {} bytes remaining in the output buffer",
                        amount, m_buf.size()));
    }

    template <class T>
    T& write()
    {
        checkRemainingSpaceAndThrow(sizeof(T));
        T* rv = reinterpret_cast<T*>(m_buf.data());
        m_buf += sizeof(T);
        return *rv;
    }

};

}