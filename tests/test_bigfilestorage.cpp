#include <gtest/gtest.h>
#include <test_bigfilestorage.hpp>

#include "FilesCleanupFixture.hpp"

#include "FileSystem.hpp"

class BigFileStorageTest : public FilesCleanupFixture {
public:
    BigFileStorageTest()
    {
        if(boost::filesystem::exists(filename))
        {
            boost::filesystem::remove(filename);
        }
    }
    boost::filesystem::path filename = "test.bin";
};

TEST_F(BigFileStorageTest, CreateOpen)
{
    {
        auto file = phkvs::FileSystem::createFileUnique(filename);
        ASSERT_TRUE(file);
        addToCleanup(filename);
        auto storage = phkvs::BigFileStorage::create(std::move(file));
    }

    {
        auto file = phkvs::FileSystem::openFileUnique(filename);
        ASSERT_TRUE(file);
        auto storage = phkvs::BigFileStorage::open(std::move(file));
    }
}

TEST_F(BigFileStorageTest, WriteRead)
{
    auto file = phkvs::FileSystem::createFileUnique(filename);
    ASSERT_TRUE(file);
    addToCleanup(filename);
    auto storage = phkvs::BigFileStorage::create(std::move(file));
    using OffsetType = phkvs::BigFileStorage::OffsetType;
    std::vector<std::pair<OffsetType, std::vector<uint8_t>>> offsetAndData;
    std::set<OffsetType> usedOffsets;
    for(size_t i=0;i<100;++i)
    {
        std::vector<uint8_t> data((i+1)*400);
        size_t j = i;
        for(auto& v:data)
        {
            v = static_cast<uint8_t>(++j);
        }
        auto offset = storage->allocateAndWrite(boost::asio::buffer(data));
        ASSERT_EQ(usedOffsets.find(offset), usedOffsets.end());
        usedOffsets.insert(offset);
        offsetAndData.emplace_back(offset, std::move(data));
    }
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        std::vector<uint8_t> readData(data.size(), 0);
        storage->read(offset, boost::asio::buffer(readData));
        EXPECT_EQ(data, readData);
    }

    //update data
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        for(auto& v:data)
        {
            ++v;
        }
        //overwrite with the same size
        storage->overwrite(offset, boost::asio::buffer(data));
    }

    //read&check
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        std::vector<uint8_t> readData(data.size(), 0);
        storage->read(offset, boost::asio::buffer(readData));
        EXPECT_EQ(data, readData);
    }

    //update and change size
    {
        size_t idx = 0;
        for(auto& p:offsetAndData)
        {
            auto offset = p.first;
            auto& data = p.second;
            if(idx<5 || (idx&1))
            {
                for(size_t i = 0; i < 400; ++i)
                {
                    data.push_back(static_cast<uint8_t>(i));
                }
            }
            else{
                data.erase(data.end() - 400);
            }
            ++idx;
            storage->overwrite(offset, boost::asio::buffer(data));
        }
    }

    //read&check
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        std::vector<uint8_t> readData(data.size(), 0);
        storage->read(offset, boost::asio::buffer(readData));
        EXPECT_EQ(data, readData);
    }

    //free slots
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        storage->free(offset);
    }

}
