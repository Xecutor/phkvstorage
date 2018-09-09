#include "FileSystem.hpp"

#ifdef _WIN32
#include "platform/win32/RandomAccessFileWin32.hpp"
#else
#include "platform/posix/RandomAccessFilePosix.hpp"
#endif

namespace phkvs{

FileSystem::UniqueFilePtr FileSystem::createFileUnique(boost::filesystem::path filename)
{
    auto handle = RandomAccessFile::create(filename);
    if(!handle)
    {
        return {};
    }
    return std::make_unique<RandomAccessFile>(filename, std::move(handle));
}

FileSystem::SharedFilePtr FileSystem::createFileShared(boost::filesystem::path filename)
{
    auto handle = RandomAccessFile::create(filename);
    if(!handle)
    {
        return {};
    }
    return std::make_shared<RandomAccessFile>(filename, std::move(handle));
}

FileSystem::UniqueFilePtr FileSystem::openFileUnique(boost::filesystem::path filename)
{
    auto handle = RandomAccessFile::open(filename);
    if(!handle)
    {
        return {};
    }
    return std::make_unique<RandomAccessFile>(filename, std::move(handle));
}

FileSystem::SharedFilePtr FileSystem::openFileShared(boost::filesystem::path filename)
{
    auto handle = RandomAccessFile::open(filename);
    if(!handle)
    {
        return {};
    }
    return std::make_shared<RandomAccessFile>(filename, std::move(handle));
}

}
