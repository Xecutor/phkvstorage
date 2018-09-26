#include <gtest/gtest.h>

#include <random>
#include <chrono>
#include <thread>

#include <fmt/format.h>

#include "StorageVolume.hpp"

#include "FilesCleanupFixture.hpp"


class TrackingSmallToMediumFileStorage : public phkvs::SmallToMediumFileStorage {
public:

    TrackingSmallToMediumFileStorage(phkvs::SmallToMediumFileStorage::UniquePtr&& impl) :
        m_impl(std::move(impl))
    {
    }

    OffsetType allocateAndWrite(boost::asio::const_buffer buf) override
    {
        auto rv = m_impl->allocateAndWrite(buf);

        m_offsetSizeMap.emplace(rv, buf.size());

        return rv;
    }

    OffsetType overwrite(OffsetType offset, size_t oldSize, boost::asio::const_buffer buf) override
    {
        auto it = m_offsetSizeMap.find(offset);
        EXPECT_NE(it, m_offsetSizeMap.end());
        EXPECT_EQ(it->second, oldSize);
        auto rv = m_impl->overwrite(offset, oldSize, buf);
        if(offset == rv)
        {
            it->second = buf.size();
        }
        else
        {
            m_offsetSizeMap.erase(it);
            m_offsetSizeMap.emplace(rv, buf.size());
        }
        return rv;
    }

    void read(OffsetType offset, boost::asio::mutable_buffer buf) override
    {
        m_impl->read(offset, buf);
    }

    void freeSlot(OffsetType offset, size_t size) override
    {
        auto it = m_offsetSizeMap.find(offset);
        EXPECT_NE(it, m_offsetSizeMap.end()) << "Attempt to double free slot at " << offset << " of size " << size;
        if(it == m_offsetSizeMap.end())
        {
            return;
        }
        EXPECT_EQ(it->second, size);
        m_offsetSizeMap.erase(it);
        m_impl->freeSlot(offset, size);
    }

    phkvs::SmallToMediumFileStorage::UniquePtr m_impl;
    std::map<OffsetType, size_t> m_offsetSizeMap;
};

class TrackingBigFileStorage : public phkvs::BigFileStorage {
public:
    TrackingBigFileStorage(phkvs::BigFileStorage::UniquePtr&& impl) :
        m_impl(std::move(impl))
    {
    }

    OffsetType allocateAndWrite(boost::asio::const_buffer buf) override
    {
        auto rv = m_impl->allocateAndWrite(buf);

        m_offsetSizeMap.emplace(rv, buf.size());

        return rv;
    }

    void overwrite(OffsetType offset, boost::asio::const_buffer buf) override
    {
        EXPECT_NE(m_offsetSizeMap.find(offset), m_offsetSizeMap.end());

        m_impl->overwrite(offset, buf);
    }

    void read(OffsetType offset, boost::asio::mutable_buffer buf) override
    {
        auto it = m_offsetSizeMap.find(offset);
        EXPECT_NE(it, m_offsetSizeMap.end());
        EXPECT_LE(it->second, buf.size());
        m_impl->read(offset, buf);
    }

    void free(OffsetType offset) override
    {
        auto it = m_offsetSizeMap.find(offset);
        EXPECT_NE(it, m_offsetSizeMap.end()) << "Attempt to double free slot at " << offset;
        if(it == m_offsetSizeMap.end())
        {
            return;
        }
        m_offsetSizeMap.erase(it);
        m_impl->free(offset);
    }

    phkvs::BigFileStorage::UniquePtr m_impl;
    std::map<OffsetType, size_t> m_offsetSizeMap;
};

class VolumeTest : public FilesCleanupFixture {
public:
    boost::filesystem::path volumeFilename = "test-volume.bin";
    boost::filesystem::path stmFilename = "test-stm.bin";
    boost::filesystem::path bigFilename = "test-big.bin";

    std::unique_ptr<phkvs::StorageVolume> volume;
    TrackingSmallToMediumFileStorage* trackingStmStoragePtr = nullptr;//owned by volume
    TrackingBigFileStorage* trackingBigStoragePtr = nullptr;//owned by volume
    std::mt19937 rng;

    void createStorageVolume()
    {
        auto mainFile = phkvs::FileSystem::createFileUnique(volumeFilename);
        ASSERT_TRUE(mainFile);
        addToCleanup(volumeFilename);
        auto stmFile = phkvs::FileSystem::createFileUnique(stmFilename);
        ASSERT_TRUE(stmFile);
        addToCleanup(stmFilename);
        auto bigFile = phkvs::FileSystem::createFileUnique(bigFilename);
        ASSERT_TRUE(bigFile);
        addToCleanup(bigFilename);
        auto stmFileStorage = phkvs::SmallToMediumFileStorage::create(std::move(stmFile));
        ASSERT_TRUE(stmFileStorage);

        auto trackingStmFileStorage = std::make_unique<TrackingSmallToMediumFileStorage>(std::move(stmFileStorage));
        trackingStmStoragePtr = trackingStmFileStorage.get();

        auto bigFileStorage = phkvs::BigFileStorage::create(std::move(bigFile));

        auto trackingBigStorage = std::make_unique<TrackingBigFileStorage>(std::move(bigFileStorage));

        trackingBigStoragePtr = trackingBigStorage.get();

        volume = phkvs::StorageVolume::create(std::move(mainFile), std::move(trackingStmFileStorage),
                                              std::move(trackingBigStorage));
    }

    VolumeTest()
    {
        std::seed_seq seed{
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()),
            static_cast<uint32_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count())};
        rng.seed(seed);
        //phkvs::StorageVolume::initFileLogger("test.log", 100000, 3);
        //phkvs::StorageVolume::initStdoutLogger();
        createStorageVolume();
    }

    template<typename T>
    void testInsertLookup(const std::string& keyPath, const T& value)
    {
        volume->store(keyPath, value);
        auto optval = volume->lookup(keyPath);
        EXPECT_TRUE(optval);
        EXPECT_EQ(boost::get<T>(*optval), value);
    }

    std::string randomString(size_t minLength, size_t maxLength)
    {
        static const char chars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::uniform_int_distribution<size_t> disLen(minLength, maxLength);
        std::string rv(disLen(rng), 0);
        std::uniform_int_distribution<size_t> disChar(0, sizeof(chars) - 2);//inclusive range, and terminating zero
        std::generate_n(rv.begin(), rv.size(), [this, &disChar]() {
            return chars[disChar(rng)];
        });
        return rv;
    }

};

TEST_F(VolumeTest, BasicInsertLookup)
{
    testInsertLookup("/foo/test-uint8_t", uint8_t{1});
    testInsertLookup("/foo/test-uint16_t", uint16_t{2});
    testInsertLookup("/foo/test-uint32_t", uint32_t{3});
    testInsertLookup("/foo/test-uint64_t", uint64_t{4});
    testInsertLookup("/foo/test-float", 5.0f);
    testInsertLookup("/foo/test-double", 6.0);
    testInsertLookup("/foo/test-string", std::string("hello world"));

    std::vector<uint8_t> dataSmall(100, 0);
    std::generate_n(dataSmall.begin(), dataSmall.size(), rng);
    testInsertLookup("/foo/test-vector-small", dataSmall);

    std::vector<uint8_t> dataMed(300, 0);
    std::generate_n(dataMed.begin(), dataMed.size(), rng);
    testInsertLookup("/foo/test-vector-med", dataMed);

    std::vector<uint8_t> dataBig(1024, 0);
    std::generate_n(dataBig.begin(), dataBig.size(), rng);

    testInsertLookup("/foo/test-vector-big", dataBig);
}

TEST_F(VolumeTest, InsertLookupLongKeys)
{
    for(size_t i = 0; i < 100; ++i)
    {
        testInsertLookup(randomString(17, 1000), randomString(1, 1000));
    }
}


TEST_F(VolumeTest, InsertMultiple)
{
    std::vector<std::pair<std::string, std::string>> keyValue;
    for(size_t i = 0; i < 10000; ++i)
    {
        auto p = std::make_pair(fmt::format("/key{}", i), fmt::format("value{}", i));
        volume->store(p.first, p.second);
        keyValue.push_back(std::move(p));
    }
    for(auto& p:keyValue)
    {
        auto val = volume->lookup(p.first);
        EXPECT_TRUE(val) << "Key " << p.first << " not found";
        EXPECT_EQ(boost::get<std::string>(*val), p.second);
    }
}

TEST_F(VolumeTest, InsertErase)
{
    std::vector<std::pair<std::string, std::string>> keyValue;
    for(size_t i = 0; i < 100; ++i)
    {
        keyValue.emplace_back(fmt::format("/key{:03}", i), fmt::format("value{}", i));
        volume->store(keyValue.back().first, keyValue.back().second);
    }
    for(auto& p:keyValue)
    {
        volume->eraseKey(p.first);
    }
    for(auto& p:keyValue)
    {
        EXPECT_FALSE(volume->lookup(p.first));
    }
}

TEST_F(VolumeTest, InsertEraseRecursive)
{
    volume->store("/dummy", "");

    std::vector<std::pair<std::string, std::string>> keyValue;
    keyValue.emplace_back("/foo/bar/key1", "value1");
    keyValue.emplace_back("/foo/bar/key2", "value2");
    keyValue.emplace_back("/foo/baz/key1", "value1");
    keyValue.emplace_back("/foo/baz/key2", "value2");
    keyValue.emplace_back("/foo/booze/key1", "value1");
    keyValue.emplace_back("/foo/booze/key2", "value2");
    keyValue.emplace_back("/foo/booze/key3", "value3");
    for(auto& p:keyValue)
    {
        volume->store(p.first, p.second);
    }
    for(auto& p:keyValue)
    {
        EXPECT_TRUE(volume->lookup(p.first));
    }
    volume->eraseDirRecursive("/foo");
    for(auto& p:keyValue)
    {
        EXPECT_FALSE(volume->lookup(p.first));
    }
}

TEST_F(VolumeTest, GetDirEntries)
{
    std::string baseDir = "/foo/bar/";
    std::set<std::string> keys;
    std::set<std::string> subDirs;
    for(size_t i = 0; i < 100; ++i)
    {
        auto key = fmt::format("key{}", i);
        volume->store(baseDir + key, static_cast<uint32_t>(i));
        keys.insert(std::move(key));
        auto subDir = fmt::format("subdir{}", i);
        volume->store(baseDir + subDir + "/key", static_cast<uint32_t>(i));
        subDirs.insert(std::move(subDir));
    }
    auto entriesOpt = volume->getDirEntries(baseDir);
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

TEST_F(VolumeTest, ExternalAllocations)
{
    auto stmAllocAtStart = trackingStmStoragePtr->m_offsetSizeMap;
    auto bigAllocAtStart = trackingBigStoragePtr->m_offsetSizeMap;
    for(size_t i=10;i<1000;++i)
    {
        volume->store(fmt::format("/foo/{}", randomString(i,i)), randomString(i,i));
        volume->store(fmt::format("/foo/{}", randomString(i,i)), std::vector<uint8_t>(i,0));
    }
    volume->eraseDirRecursive("/foo");
    EXPECT_EQ(stmAllocAtStart, trackingStmStoragePtr->m_offsetSizeMap);
    EXPECT_EQ(bigAllocAtStart, trackingBigStoragePtr->m_offsetSizeMap);
}

TEST_F(VolumeTest, Expiration)
{
    const auto key1 = "/expiresInSecond";
    const auto key2 = "/expiresInTwoSeconds";
    volume->storeExpiring(key1, uint8_t(1), std::chrono::system_clock::now() + std::chrono::seconds(1));
    volume->storeExpiring(key2, uint8_t(2), std::chrono::system_clock::now() + std::chrono::seconds(2));
    EXPECT_TRUE(volume->lookup(key1));
    EXPECT_TRUE(volume->lookup(key2));
    auto dirOpt = volume->getDirEntries("/");
    ASSERT_TRUE(dirOpt);
    EXPECT_EQ(dirOpt->size(), 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(volume->lookup(key1));
    EXPECT_TRUE(volume->lookup(key2));
    dirOpt = volume->getDirEntries("/");
    ASSERT_TRUE(dirOpt);
    EXPECT_EQ(dirOpt->size(), 2);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_FALSE(volume->lookup(key1));
    EXPECT_TRUE(volume->lookup(key2));
    dirOpt = volume->getDirEntries("/");
    ASSERT_TRUE(dirOpt);
    EXPECT_EQ(dirOpt->size(), 1);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_FALSE(volume->lookup(key1));
    EXPECT_FALSE(volume->lookup(key2));
    dirOpt = volume->getDirEntries("/");
    ASSERT_TRUE(dirOpt);
    EXPECT_EQ(dirOpt->size(), 0);
}

TEST_F(VolumeTest, OverwriteException)
{
    volume->store("/dir/key", uint8_t{1});
    EXPECT_THROW(volume->store("/dir/key/key2", uint8_t{1}), std::runtime_error);
}
