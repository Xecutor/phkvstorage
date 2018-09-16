#include <gtest/gtest.h>
#include <SmallToMediumFileStorage.hpp>
#include <test_bigfilestorage.hpp>
#include <StorageVolume.hpp>

#include "FilesCleanupFixture.hpp"

#include "FileSystem.hpp"

class VolumeTest : public FilesCleanupFixture {
public:
    boost::filesystem::path volumeFilename = "test-volume.bin";
    boost::filesystem::path stmFilename = "test-stm.bin";
    boost::filesystem::path bigFilename = "test-big.bin";

    std::unique_ptr<phkvs::StorageVolume> volume;

    void createStorageVolume()
    {
        auto mainFile = phkvs::FileSystem::createFileUnique(volumeFilename);
        ASSERT_NO_FATAL_FAILURE(mainFile);
        addToCleanup(volumeFilename);
        auto stmFile = phkvs::FileSystem::createFileUnique(stmFilename);
        ASSERT_NO_FATAL_FAILURE(stmFile);
        addToCleanup(stmFilename);
        auto bigFile = phkvs::FileSystem::createFileUnique(bigFilename);
        ASSERT_NO_FATAL_FAILURE(bigFile);
        addToCleanup(bigFilename);
        volume = phkvs::StorageVolume::create(std::move(mainFile), std::move(stmFile), std::move(bigFile));
    }

    VolumeTest()
    {
        createStorageVolume();
    }

};

TEST_F(VolumeTest, BasicInsertLookup)
{
    volume->store("/foo/test-uint8_t", uint8_t{1});
    auto val = volume->lookup("/foo/test-uint8_t");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<uint8_t>(*val), 1);

    volume->store("/foo/test-uint16_t", uint16_t{2});
    val = volume->lookup("/foo/test-uint16_t");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<uint16_t>(*val), 2);

    volume->store("/foo/test-uint32_t", uint32_t{3});
    val = volume->lookup("/foo/test-uint32_t");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<uint32_t>(*val), 3);

    volume->store("/foo/test-uint64_t", uint64_t{4});
    val = volume->lookup("/foo/test-uint64_t");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<uint64_t>(*val), 4);

    volume->store("/foo/test-float", 5.0f);
    val = volume->lookup("/foo/test-float");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<float>(*val), 5.0f);

    volume->store("/foo/test-double", 6.0);
    val = volume->lookup("/foo/test-double");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<double>(*val), 6.0);

    volume->store("/foo/test-string", "hello world");
    val = volume->lookup("/foo/test-string");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<std::string>(*val), "hello world");

    std::vector<uint8_t> data(1024, 0);
    uint8_t v = 0;
    for(auto& b : data)
    {
        b = ++v;
    }
    volume->store("/foo/test-vector", data);
    val = volume->lookup("/foo/test-vector");
    ASSERT_TRUE(val);
    ASSERT_EQ(boost::get<std::vector<uint8_t>>(*val), data);
}

TEST_F(VolumeTest, InsertMultiple)
{
    std::vector<std::pair<std::string, std::string>> keyValue;
    for(size_t i = 0; i < 1000; ++i)
    {
        auto p = std::make_pair(fmt::format("/key{}", i), fmt::format("value{}", i));
        volume->store(p.first, p.second);
        keyValue.push_back(std::move(p));
    }
    for(auto& p:keyValue)
    {
        auto val = volume->lookup(p.first);
        ASSERT_TRUE(val);
        ASSERT_EQ(boost::get<std::string>(*val), p.second);
    }
}