#include <gtest/gtest.h>
#include "InputBinBuffer.hpp"
#include "OutputBinBuffer.hpp"

TEST(Input, BasicInts)
{
    const uint8_t data[] = {
        1,
        2, 1,
        4, 3, 2, 1,
        8,7,6,5,4,3,2,1
    };
    phkvs::InputBinBuffer in(boost::asio::buffer(data));
    ASSERT_EQ(in.readU8(), 0x01);
    ASSERT_EQ(in.readU16(), 0x0102);
    ASSERT_EQ(in.readU32(), 0x01020304);
    ASSERT_EQ(in.readU64(), 0x0102030405060708);
    ASSERT_EQ(in.remainingSpace(), 0);

    EXPECT_THROW(in.readU8(), std::out_of_range);
    EXPECT_THROW(in.readU16(), std::out_of_range);
    EXPECT_THROW(in.readU32(), std::out_of_range);
    EXPECT_THROW(in.readU64(), std::out_of_range);
    EXPECT_THROW(in.readFloat(), std::out_of_range);
    EXPECT_THROW(in.readDouble(), std::out_of_range);
}

TEST(Output, BasicInts)
{
    std::vector<uint8_t> outData(1 + 2 + 4 + 8);
    const std::vector<uint8_t> expectedData = {
        1,
        2, 1,
        4, 3, 2, 1,
        8, 7, 6, 5, 4, 3, 2, 1
    };
    phkvs::OutputBinBuffer out(boost::asio::buffer(outData));
    out.writeU8(1);
    out.writeU16(0x0102);
    out.writeU32(0x01020304);
    out.writeU64(0x0102030405060708);
    ASSERT_EQ(outData, expectedData);
    ASSERT_EQ(out.remainingSpace(), 0);

    EXPECT_THROW(out.writeU8(1), std::out_of_range);
    EXPECT_THROW(out.writeU16(1), std::out_of_range);
    EXPECT_THROW(out.writeU32(1), std::out_of_range);
    EXPECT_THROW(out.writeU64(1), std::out_of_range);
    EXPECT_THROW(out.writeFloat(1.0f), std::out_of_range);
    EXPECT_THROW(out.writeDouble(1.0), std::out_of_range);
}

TEST(InputOutput, AllTypes)
{
    uint8_t u8 = 0x12;
    uint16_t u16 = 0x4567;
    uint32_t u32 = 0x89012345;
    uint64_t u64 = 0x6789012345678901;
    float f = 234.567f;
    double d = 8901.2345;
    std::vector<uint8_t> outData(sizeof(u8) + sizeof(u16) + sizeof(u32) + sizeof(u64) + sizeof(f) + sizeof(d));

    phkvs::OutputBinBuffer out(boost::asio::buffer(outData));
    out.writeU8(u8);
    out.writeU16(u16);
    out.writeU32(u32);
    out.writeU64(u64);
    out.writeFloat(f);
    out.writeDouble(d);

    ASSERT_EQ(out.remainingSpace(), 0);

    phkvs::InputBinBuffer in(boost::asio::buffer(outData));

    ASSERT_EQ(in.readU8(), u8);
    ASSERT_EQ(in.readU16(), u16);
    ASSERT_EQ(in.readU32(), u32);
    ASSERT_EQ(in.readU64(), u64);
    ASSERT_EQ(in.readFloat(), f);
    ASSERT_EQ(in.readDouble(), d);

    ASSERT_EQ(in.remainingSpace(), 0);

}