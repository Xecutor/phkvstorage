#pragma once

#include <memory>

#include <boost/filesystem.hpp>

#include "IRandomAccessFile.hpp"

namespace phkvs{

class FileSystem{
public:
    using UniqueFilePtr = std::unique_ptr<IRandomAccessFile>;
    using SharedFilePtr = std::shared_ptr<IRandomAccessFile>;
    static UniqueFilePtr createFileUnique(boost::filesystem::path filename);
    static SharedFilePtr createFileShared(boost::filesystem::path filename);
    static UniqueFilePtr openFileUnique(boost::filesystem::path  filename);
    static SharedFilePtr openFileShared(boost::filesystem::path  filename);

    static int getLastError();
};

}