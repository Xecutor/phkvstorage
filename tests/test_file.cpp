#include <gtest/gtest.h>

#include "FilesCleanupFixture.hpp"

#include "FileSystem.hpp"

#include <boost/filesystem.hpp>

class Files : public FilesCleanupFixture{
public:
};

TEST_F(Files, CreateReadWrite)
{
    boost::filesystem::path fileName = "test.bin";
    auto file = phkvs::FileSystem::createFileUnique(fileName);
    ASSERT_TRUE(file) << "Failed to create file " << fileName;

    addToCleanup(fileName);

    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    auto bufOut = boost::asio::buffer(data);
    file->write(bufOut);
    file->seek(0);
    std::vector<uint8_t> dataRead(data.size());
    auto bufIn = boost::asio::buffer(dataRead);
    file->read(bufIn);
    EXPECT_EQ(data, dataRead);

    auto bufTmp = boost::asio::buffer(dataRead);

    EXPECT_THROW(file->seek(128), std::runtime_error);
    EXPECT_THROW(file->read(bufTmp), std::runtime_error);
}

TEST_F(Files, OpenRead)
{
    boost::filesystem::path fileName = "test.bin";
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    {
        auto file = phkvs::FileSystem::createFileUnique(fileName);
        ASSERT_TRUE(file) << "Failed to create file " << fileName;

        addToCleanup(fileName);

        auto bufOut = boost::asio::buffer(data);
        file->write(bufOut);
    }
    {
        auto file = phkvs::FileSystem::openFileUnique(fileName);

        ASSERT_TRUE(file) << "Failed to open file " << fileName;

        std::vector<uint8_t> dataRead(data.size());
        auto bufIn = boost::asio::buffer(dataRead);
        file->read(bufIn);
        EXPECT_EQ(data, dataRead);
    }
}
