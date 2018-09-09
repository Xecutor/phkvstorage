#include <gtest/gtest.h>

#include "UIntArrayHexFormatter.hpp"
#include "FileVersion.hpp"

TEST(SimpleTest, FormatIntArray)
{
    std::array<uint8_t, 6> arr{0x01, 0x02, 0x11,0x22,0x33,0x44};

    EXPECT_EQ(fmt::format("{}", arr), "[01, 02, 11, 22, 33, 44]");

    std::array<uint16_t, 2> arr2{0x0001,0x0002};
    EXPECT_EQ(fmt::format("{}", arr2), "[0001, 0002]");

    //std::array<std::string, 1> v{""};
    //fmt::format("{}", v);
}

TEST(SimpleTest, FileVersion)
{
    phkvs::FileVersion v{1,2};
    EXPECT_EQ(fmt::format("{}", v), "v1.2");
}
