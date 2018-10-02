#include <gtest/gtest.h>

#include <random>
#include <chrono>
#include <thread>

#include <fmt/format.h>

#include "PHKVStorage.hpp"

#include "FilesCleanupFixture.hpp"

class PHKVStorageTest : public FilesCleanupFixture {
public:
    phkvs::PHKVStorage::UniquePtr storage;

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
    phkvs::PHKVStorage::VolumeId createMountAndCleanVolume(const boost::filesystem::path& volumePath, boost::string_view volumeName,
                                                      boost::string_view mountPointPath)
    {
        auto rv = storage->createAndMountVolume(volumePath, volumeName, mountPointPath);
        addVolumeToCleanup(volumePath, std::string(volumeName.data(), volumeName.length()));
        return rv;
    }
};

TEST_F(PHKVStorageTest, createUnmountMount)
{
    createStorage();

    auto volId = createMountAndCleanVolume(".", "test", "/");

    storage->store("/hello", "world");
    EXPECT_TRUE(storage->lookup("/hello"));
    storage->unmountVolume(volId);
    EXPECT_FALSE(storage->lookup("/hello"));

    storage->mountVolume(".", "test", "/");
    EXPECT_TRUE(storage->lookup("/hello"));
}

TEST_F(PHKVStorageTest, mountMultiple)
{
    createStorage();

    createMountAndCleanVolume(".", "test1", "/foo");

    createMountAndCleanVolume(".", "test2", "/bar");

    storage->store("/foo/key1", "value1");
    EXPECT_TRUE(storage->lookup("/foo/key1"));
    storage->store("/bar/key2", "value2");
    EXPECT_TRUE(storage->lookup("/bar/key2"));
    EXPECT_THROW(storage->store("/baz/key3", "value3"), std::runtime_error);

}

TEST_F(PHKVStorageTest, mountMultiplePrio)
{
    createStorage();
    auto volId = createMountAndCleanVolume(".", "test1", "/foo/bar");
    createMountAndCleanVolume(".", "test2", "/foo");

    storage->store("/foo/bar/hello", "world");
    EXPECT_TRUE(storage->lookup("/foo/bar/hello"));
    storage->unmountVolume(volId);
    EXPECT_FALSE(storage->lookup("/foo/bar/hello"));
}

TEST_F(PHKVStorageTest, mountMultipleSameMany)
{
    phkvs::PHKVStorage::Options opt;
    opt.cachePoolSize = 200000;
    createStorage(opt);
    std::vector<std::string> volumes;
    std::vector<std::pair<std::string, uint32_t>> keysValues;
    for(size_t i = 0; i < 100; ++i)
    {
        auto volName = fmt::format("vol{}", i);
        auto volId = createMountAndCleanVolume(".", volName, "/foo");
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

TEST_F(PHKVStorageTest, erase)
{
    createStorage();
    auto volId = createMountAndCleanVolume(".", "test", "/");

    storage->store("/key", "value");
    EXPECT_TRUE(storage->lookup("/key"));
    storage->eraseKey("/key");
    EXPECT_FALSE(storage->lookup("/key"));
    storage->unmountVolume(volId);
    storage->mountVolume(".", "test", "/");
    EXPECT_FALSE(storage->lookup("/key"));
}

TEST_F(PHKVStorageTest, eraseRecursiveBasic)
{
    createStorage();
    auto volId = createMountAndCleanVolume(".", "test", "/");

    storage->store("/foo/key1", "value1");
    storage->store("/foo/key2", "value2");
    storage->store("/foo/bar/key1", "value1");
    storage->store("/foo/bar/key2", "value2");
    EXPECT_TRUE(storage->lookup("/foo/key1"));
    EXPECT_TRUE(storage->lookup("/foo/key2"));
    EXPECT_TRUE(storage->lookup("/foo/bar/key1"));
    EXPECT_TRUE(storage->lookup("/foo/bar/key2"));
    storage->eraseDirRecursive("/foo");
    EXPECT_FALSE(storage->lookup("/foo/key1"));
    EXPECT_FALSE(storage->lookup("/foo/key2"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key1"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key2"));
    storage->unmountVolume(volId);
    storage->mountVolume(".", "test", "/");
    EXPECT_FALSE(storage->lookup("/foo/key1"));
    EXPECT_FALSE(storage->lookup("/foo/key2"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key1"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key2"));
}

TEST_F(PHKVStorageTest, eraseRecursiveMultiple)
{
    createStorage();
    auto volId1 = createMountAndCleanVolume(".", "test1", "/");
    auto volId2 = createMountAndCleanVolume(".", "test2", "/foo");
    storage->store("/foo/key1", "value1");
    storage->store("/foo/key2", "value2");
    storage->store("/foo/bar/key1", "value1");
    storage->store("/foo/bar/key2", "value2");
    EXPECT_TRUE(storage->lookup("/foo/key1"));
    EXPECT_TRUE(storage->lookup("/foo/key2"));
    EXPECT_TRUE(storage->lookup("/foo/bar/key1"));
    EXPECT_TRUE(storage->lookup("/foo/bar/key2"));
    storage->eraseDirRecursive("/foo");
    EXPECT_FALSE(storage->lookup("/foo/key1"));
    EXPECT_FALSE(storage->lookup("/foo/key2"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key1"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key2"));
    storage->unmountVolume(volId1);
    storage->unmountVolume(volId2);
    storage->mountVolume(".", "test1", "/");
    storage->mountVolume(".", "test2", "/");
    EXPECT_FALSE(storage->lookup("/foo/key1"));
    EXPECT_FALSE(storage->lookup("/foo/key2"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key1"));
    EXPECT_FALSE(storage->lookup("/foo/bar/key2"));
}

TEST_F(PHKVStorageTest, getDirEntriesBasic)
{
    createStorage();
    createMountAndCleanVolume(".", "test", "/");

    std::string baseDir = "/foo/bar/";
    std::set<std::string> keys;
    std::set<std::string> subDirs;
    for(size_t i = 0; i < 100; ++i)
    {
        auto key = fmt::format("key{}", i);
        storage->store(baseDir + key, static_cast<uint32_t>(i));
        keys.insert(std::move(key));
        auto subDir = fmt::format("subdir{}", i);
        storage->store(baseDir + subDir + "/key", static_cast<uint32_t>(i));
        subDirs.insert(std::move(subDir));
    }
    auto entriesOpt = storage->getDirEntries(baseDir);
    ASSERT_TRUE(entriesOpt);
    for(auto& entry:*entriesOpt)
    {
        using EntryType = phkvs::PHKVStorage::EntryType;
        if(entry.type == EntryType::key)
        {
            auto it = keys.find(entry.name);
            EXPECT_NE(it, keys.end());
            keys.erase(it);
        }
        else if(entry.type == EntryType::dir)
        {
            auto it = subDirs.find(entry.name);
            EXPECT_NE(it, subDirs.end());
            subDirs.erase(it);
        }
        else
        {
            GTEST_FAIL() << "Unexpected entry.type value" << static_cast<int>(entry.type);
        }
    }
    EXPECT_TRUE(keys.empty());
    EXPECT_TRUE(subDirs.empty());
}
