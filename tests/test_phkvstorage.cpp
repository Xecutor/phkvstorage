#include <gtest/gtest.h>

#include <random>
#include <chrono>
#include <thread>

#include <fmt/format.h>

#include "PHKVStorage.hpp"

#include "FilesCleanupFixture.hpp"

class PHKVStorageTest : public FilesCleanupFixture {
public:
    std::unique_ptr<phkvs::PHKVStorage> storage;

    void createStorage(phkvs::PHKVStorage::Options opt={})
    {
        storage = phkvs::PHKVStorage::create(opt);
        ASSERT_TRUE(storage);
    }

    void addVolumeToCleanup(boost::filesystem::path path, std::string volumeName)
    {
        addToCleanup(path / (volumeName + ".phkvsmain"));
        addToCleanup(path / (volumeName + ".phkvsbig"));
        addToCleanup(path / (volumeName + ".phkvsstm"));
    }
};

TEST_F(PHKVStorageTest, createAndMountUnmount)
{
    createStorage();

    auto volId = storage->createAndMountVolume(".", "test", "/");
    addVolumeToCleanup(".", "test");
    storage->store("/hello", "world");
    EXPECT_TRUE(storage->lookup("/hello"));
    storage->unmountVolume(volId);
    EXPECT_FALSE(storage->lookup("/hello"));

    storage->mountVolume(".", "test", "/");
    EXPECT_TRUE(storage->lookup("/hello"));
}

TEST_F(PHKVStorageTest, createAndMountMultiple)
{
    createStorage();

    storage->createAndMountVolume(".", "test1", "/foo");
    addVolumeToCleanup(".", "test1");

    storage->createAndMountVolume(".", "test2", "/bar");
    addVolumeToCleanup(".", "test2");

    storage->store("/foo/key1", "value1");
    EXPECT_TRUE(storage->lookup("/foo/key1"));
    storage->store("/bar/key2", "value2");
    EXPECT_TRUE(storage->lookup("/bar/key2"));
    EXPECT_THROW(storage->store("/baz/key3", "value3"), std::runtime_error);

}

TEST_F(PHKVStorageTest, createAndMountMultiplePrio)
{
    createStorage();
    auto volId = storage->createAndMountVolume(".", "test1", "/foo/bar");
    addVolumeToCleanup(".", "test1");
    storage->createAndMountVolume(".", "test2", "/foo");
    addVolumeToCleanup(".", "test2");
    storage->store("/foo/bar/hello", "world");
    EXPECT_TRUE(storage->lookup("/foo/bar/hello"));
    storage->unmountVolume(volId);
    EXPECT_FALSE(storage->lookup("/foo/bar/hello"));
}

TEST_F(PHKVStorageTest, createAndMountMultipleSameMP)
{
    createStorage({200000});
    std::vector<std::string> volumes;
    std::vector<std::pair<std::string, uint32_t>> keysValues;
    for(size_t i = 0; i < 1000; ++i)
    {
        auto volName = fmt::format("vol{}", i);
        auto volId = storage->createAndMountVolume(".", volName, "/foo");
        addVolumeToCleanup(".", volName);
        for(size_t j = 0; j < 100; ++j)
        {
            auto key = fmt::format("/foo/vol{}-key{}", i, j);
            uint32_t value = i * 1000 + j;
            storage->store(key, value);
            keysValues.emplace_back(std::move(key), value);
        }
        volumes.push_back(std::move(volName));
        storage->unmountVolume(volId);
    }
    for(auto& volName : volumes)
    {
        storage->mountVolume(".", volName, "/foo");
    }
    for(auto& kv:keysValues)
    {
        auto& key = kv.first;
        auto value = kv.second;
        auto valOpt = storage->lookup(key);
        EXPECT_TRUE(valOpt);
        if(valOpt)
        {
            EXPECT_EQ(boost::get<uint32_t>(*valOpt), value);
        }
    }
}