#include <gtest/gtest.h>

#include "FilesCleanupFixture.hpp"

#include "FileSystem.hpp"
#include "SmallToMediumFileStorage.hpp"


#include <boost/filesystem.hpp>

class SmallToMediumStorageTest : public FilesCleanupFixture {
};

TEST_F(SmallToMediumStorageTest, CreateOpen)
{
    boost::filesystem::path filename = "test.bin";
    {
        phkvs::SmallToMediumFileStorage storage(phkvs::FileSystem::createFileUnique(filename));
        addToCleanup(filename);
        storage.create();
    }

    {
        phkvs::SmallToMediumFileStorage storage(phkvs::FileSystem::openFileUnique(filename));
        storage.open();
    }
}

TEST_F(SmallToMediumStorageTest, CreateWriteRead)
{
    boost::filesystem::path filename = "test.bin";
    phkvs::SmallToMediumFileStorage storage(phkvs::FileSystem::createFileUnique(filename));
    addToCleanup(filename);
    storage.create();
    using OffsetType = phkvs::SmallToMediumFileStorage::OffsetType;
    std::vector<std::pair<OffsetType, std::vector<uint8_t>>> offsetAndData;
    std::set<OffsetType> usedOffsets;
    constexpr size_t slotSizeInc = phkvs::SmallToMediumFileStorage::slotSizeIncrement();
    constexpr size_t maxDataSize = phkvs::SmallToMediumFileStorage::maxDataSize();

    //fill with some data
    for(size_t i = 1; i <= maxDataSize; ++i)
    {
        std::vector<uint8_t> data(i);
        size_t j = i;
        for(auto& v:data)
        {
            v = static_cast<uint8_t>(++j);
        }
        auto offset = storage.allocateAndWrite(boost::asio::buffer(data));
        ASSERT_EQ(usedOffsets.find(offset), usedOffsets.end());
        usedOffsets.insert(offset);
        offsetAndData.emplace_back(offset, std::move(data));
    }

    //read data back
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        std::vector<uint8_t> readData(data.size(), 0);
        storage.read(offset, boost::asio::buffer(readData));
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
        EXPECT_EQ(offset, storage.overwrite(offset, data.size(), boost::asio::buffer(data)));
    }

    //read&check
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        std::vector<uint8_t> readData(data.size(), 0);
        storage.read(offset, boost::asio::buffer(readData));
        EXPECT_EQ(data, readData);
    }

    //update and change size
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        auto oldSize = data.size();
        //slots of size<16 won't change and
        if(data.size() <= slotSizeInc ||
           data.size() > maxDataSize - slotSizeInc)
        {
            continue;
        }
        for(size_t i = 0; i < slotSizeInc; ++i)
        {
            data.push_back(static_cast<uint8_t>(i));
        }
        //overwrite with bigger data
        OffsetType newOffset = storage.overwrite(offset, oldSize, boost::asio::buffer(data));
        EXPECT_NE(offset, newOffset) << "oldSize=="<<oldSize<<", newSize="<<data.size();
        p.first = newOffset;
        usedOffsets.insert(newOffset);
    }

    //read&check
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        std::vector<uint8_t> readData(data.size(), 0);
        storage.read(offset, boost::asio::buffer(readData));
        EXPECT_EQ(data, readData);
    }

    //free slots
    for(auto& p:offsetAndData)
    {
        auto offset = p.first;
        auto& data = p.second;
        storage.freeSlot(offset, data.size());
    }

    //check that free worked
    for(auto& p:offsetAndData)
    {
        auto& data = p.second;
        auto offset = storage.allocateAndWrite(boost::asio::buffer(data));
        //we freed all previously used offsets, so all new allocations
        //should reuse previous offsets.
        EXPECT_NE(usedOffsets.find(offset), usedOffsets.end());
    }
}
