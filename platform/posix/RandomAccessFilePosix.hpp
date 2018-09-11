#pragma once

#include "IRandomAccessFile.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <boost/filesystem.hpp>
#include <fmt/format.h>

namespace phkvs{

class RandomAccessFile : public IRandomAccessFile {
    friend class FileSystem;

    static_assert(sizeof(OffsetType) == sizeof(off_t), "Expecting 64-bit file offset type");

    struct Handle {
        int m_handle;

        static const int invalidValue = -1;

        Handle(int handle) : m_handle(handle)
        {
        }

        Handle(const Handle&) = delete;

        Handle(Handle&& other) noexcept : m_handle(other.m_handle)
        {
            other.m_handle = invalidValue;
        }

        Handle& operator=(const Handle& other) = delete;

        Handle& operator=(Handle&& other)
        {
            if(this == &other)
            {
                return *this;
            }
            close();
            m_handle = other.m_handle;
            other.m_handle = invalidValue;
            return *this;
        }

        int get()
        {
            return m_handle;
        }

        explicit operator bool() const
        {
            return m_handle != invalidValue;
        }

        void close()
        {
            if(m_handle != invalidValue)
            {
                ::close(m_handle);
                m_handle = invalidValue;
            }
        }

        ~Handle()
        {
            close();
        }
    };

public:

    RandomAccessFile(boost::filesystem::path filename, Handle&& handle) :
      m_filename(std::move(filename)), m_handle(std::move(handle))
    {
    }

    ~RandomAccessFile() override = default;

    void read(boost::asio::mutable_buffer buf) override
    {
        int ret = ::read(m_handle.get(), buf.data(), buf.size());
        if(ret == -1)
        {
            int err = errno;
            throw fmt::system_error(err, "[{}]read error", m_filename.string());
        }
        if(buf.size() != ret)
        {
            throw std::runtime_error(
                fmt::format("[{}]read requested {} bytes, but actually read {}",
                    m_filename.string(), buf.size(), ret));
        }
    }

    void write(boost::asio::const_buffer buf) override
    {
        int ret = ::write(m_handle.get(), buf.data(), buf.size());
        if(ret == -1)
        {
            int err = errno;
            throw fmt::system_error(err, "[{}]write error", m_filename.string());
        }
        if(buf.size() != ret)
        {
            throw std::runtime_error(
                fmt::format("[{}]write requested {} bytes, but actually written {}",
                    m_filename.string(), buf.size(), ret));
        }
    }

    OffsetType seekEnd() override
    {
        int ret = lseek(m_handle.get(), 0, SEEK_END);

        if(ret < 0 )
        {
            int err = errno;
            throw fmt::system_error(err, "[{}]seekEnd error", m_filename.string());
        }
        return static_cast<OffsetType>(ret);
    }

    void seek(OffsetType offset) override
    {
        auto fileSize = seekEnd();
        if(offset > fileSize)
        {
            throw std::runtime_error(
                fmt::format("[{}]seek attempt to set file position to {}, beyond file size {}",
                            m_filename.string(), offset, fileSize));
        }

        auto ret = lseek(m_handle.get(), offset, SEEK_SET);
        if(ret == static_cast<off_t>(-1))
        {
            int err = errno;
            throw fmt::system_error(err, "[{}]seek error", m_filename.string());
        }
    }

    const boost::filesystem::path& getFilename()const override
    {
        return m_filename;
    }

private:

    static Handle open(const boost::filesystem::path& path)
    {
        return {::open(path.native().c_str(),O_RDWR)};
    }

    static Handle create(const boost::filesystem::path& path)
    {
        return {::open(path.native().c_str(), O_CREAT|O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)};
    }

    boost::filesystem::path m_filename;
    Handle m_handle;
};

}