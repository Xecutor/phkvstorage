#pragma once

#include "IRandomAccessFile.hpp"

#define WIN32_LEAN_AND_MEAN
#include <Windows.h>

#include <boost/filesystem.hpp>
#include <fmt/format.h>
#include <stdexcept>

namespace phkvs {

class RandomAccessFile : public IRandomAccessFile {
    friend class FileSystem;

    struct Handle {
        HANDLE m_handle;

        Handle(HANDLE handle) : m_handle(handle)
        {
        }

        Handle(const Handle&) = delete;

        Handle(Handle&& other) noexcept : m_handle(other.m_handle)
        {
            other.m_handle = INVALID_HANDLE_VALUE;
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
            other.m_handle = INVALID_HANDLE_VALUE;
            return *this;
        }

        HANDLE get()
        {
            return m_handle;
        }

        explicit operator bool() const
        {
            return m_handle != INVALID_HANDLE_VALUE;
        }

        void close()
        {
            if(m_handle != INVALID_HANDLE_VALUE)
            {
                CloseHandle(m_handle);
                m_handle = INVALID_HANDLE_VALUE;
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

    void read(boost::asio::mutable_buffer& buf) override
    {
        DWORD actuallyRead = 0;
        if(!ReadFile(m_handle.get(), buf.data(), buf.size(), &actuallyRead, nullptr))
        {
            auto error = static_cast<int>(GetLastError());
            throw fmt::windows_error(error, "[{}]read error", m_filename.string());
        }
        if(buf.size() != actuallyRead)
        {
            auto error = static_cast<int>(GetLastError());
            throw fmt::windows_error(error, "[{}]read requested {} bytes, but actually read {}",
                m_filename.string(), buf.size(), actuallyRead);
        }
    }

    void write(boost::asio::const_buffer buf) override
    {
        DWORD actuallyWritten = 0;
        if(!WriteFile(m_handle.get(), buf.data(), buf.size(), &actuallyWritten, nullptr))
        {
            auto error = static_cast<int>(GetLastError());
            throw fmt::windows_error(error, "[{}]write error", m_filename.string());
        }
        if(buf.size() != actuallyWritten)
        {
            auto error = static_cast<int>(GetLastError());
            throw fmt::windows_error(error, "[{}]write requested {} bytes, but actually written {}",
                m_filename.string(), buf.size(), actuallyWritten);
        }
    }

    OffsetType seekEnd() override
    {
        LARGE_INTEGER liOffset;
        LARGE_INTEGER liFileSize;

        liOffset.QuadPart = 0;

        if(!SetFilePointerEx(m_handle.get(), liOffset, &liFileSize, FILE_END))
        {
            auto error = static_cast<int>(GetLastError());
            throw fmt::windows_error(error, "[{}]seekEnd error", m_filename.string());
        }
        return static_cast<OffsetType>(liFileSize.QuadPart);
    }

    void seek(OffsetType offset) override
    {
        LARGE_INTEGER liOffset;

        auto fileSize = seekEnd();
        if(offset > fileSize)
        {
            throw std::runtime_error(
                fmt::format("[{}]Attempt to set file position to {}, beyond file size {}",
                            m_filename.string(), offset, fileSize));
        }

        liOffset.QuadPart = offset;
        if(!SetFilePointerEx(m_handle.get(), liOffset, nullptr, FILE_BEGIN))
        {
            auto error = static_cast<int>(GetLastError());
            throw fmt::windows_error(error, "[{}]seek error", m_filename.string());
        }
    }

    const boost::filesystem::path& getFilename()const override
    {
        return m_filename;
    }

private:

    static Handle open(const boost::filesystem::path& path)
    {
        return {CreateFileW(
            path.native().c_str(),
            GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
            nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr)};
    }

    static Handle create(const boost::filesystem::path& path)
    {
        return {CreateFileW(
            path.native().c_str(),
            GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
            nullptr, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, nullptr)};
    }

    boost::filesystem::path m_filename;
    Handle m_handle;
};

}