#include <gtest/gtest.h>
#include <FileMagic.hpp>
#include <boost/utility/string_view.hpp>

#include "UIntArrayHexFormatter.hpp"
#include "StringViewFormatter.hpp"
#include "FileVersion.hpp"
#include "KeyPathUtil.hpp"

TEST(SimpleTest, FormatIntArray)
{
    std::array<uint8_t, 6> arr{0x01, 0x02, 0x11, 0x22, 0x33, 0x44};

    EXPECT_EQ(fmt::format("{}", arr), "[01, 02, 11, 22, 33, 44]");

    std::array<uint16_t, 2> arr2{0x0001, 0x0002};
    EXPECT_EQ(fmt::format("{}", arr2), "[0001, 0002]");

    //std::array<std::string, 1> v{""};
    //fmt::format("{}", v);
}

TEST(SimpleTest, FileVersion)
{
    phkvs::FileVersion v{1, 2};
    EXPECT_EQ(fmt::format("{}", v), "v1.2");
    std::array<uint8_t, phkvs::FileVersion::binSize()> data;
    phkvs::OutputBinBuffer out(boost::asio::buffer(data));
    v.serialize(out);
    phkvs::FileVersion v2;
    phkvs::InputBinBuffer in(boost::asio::buffer(data));
    v2.deserialize(in);
    EXPECT_EQ(v, v2);
}

TEST(SimpleTest, FileMagic)
{
    phkvs::FileMagic m{{'A', 'B', 'C', 'D'}};
    EXPECT_EQ(fmt::format("{}", m), "[41, 42, 43, 44]");

    std::array<uint8_t, phkvs::FileMagic::binSize()> data;
    phkvs::OutputBinBuffer out(boost::asio::buffer(data));
    m.serialize(out);
    phkvs::FileMagic m2;
    phkvs::InputBinBuffer in(boost::asio::buffer(data));
    m2.deserialize(in);
    EXPECT_EQ(m, m2);
}

TEST(SimpleTest, BoostStringView)
{
    boost::string_view sv{"hello"};
    EXPECT_EQ(fmt::format("{}", sv), "hello");
}

TEST(SimpleTest, KeyPathUtil)
{
    auto pathVector = phkvs::splitKeyPath("/foo/bar");
    EXPECT_EQ(pathVector[0], "foo");
    EXPECT_EQ(pathVector[1], "bar");

    pathVector = phkvs::splitKeyPath("foo/bar");
    EXPECT_EQ(pathVector[0], "foo");
    EXPECT_EQ(pathVector[1], "bar");

    pathVector = phkvs::splitKeyPath("foo/bar/");
    EXPECT_EQ(pathVector[0], "foo");
    EXPECT_EQ(pathVector[1], "bar");

    pathVector = phkvs::splitKeyPath("/foo/bar/");
    EXPECT_EQ(pathVector[0], "foo");
    EXPECT_EQ(pathVector[1], "bar");

    pathVector = phkvs::splitKeyPath("/foo");
    EXPECT_EQ(pathVector[0], "foo");

    pathVector = phkvs::splitKeyPath("foo");
    EXPECT_EQ(pathVector[0], "foo");

    pathVector = phkvs::splitKeyPath("foo/");
    EXPECT_EQ(pathVector[0], "foo");

    pathVector = phkvs::splitKeyPath("/foo/bar/baz");
    EXPECT_EQ(pathVector[0], "foo");
    EXPECT_EQ(pathVector[1], "bar");
    EXPECT_EQ(pathVector[2], "baz");
}
