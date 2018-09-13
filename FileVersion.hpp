#pragma once

#include <stdint.h>

#include "InputBinBuffer.hpp"
#include "OutputBinBuffer.hpp"

namespace phkvs{

struct FileVersion{
    uint16_t major;
    uint16_t minor;

    static constexpr size_t binSize()
    {
        return sizeof(major) + sizeof(minor);
    }

    void serialize(OutputBinBuffer& out)const
    {
        out.writeU16(major);
        out.writeU16(minor);
    }

    void deserialize(InputBinBuffer& in)
    {
        major = in.readU16();
        minor = in.readU16();
    }

    friend bool operator==(const FileVersion& left, const FileVersion& right)
    {
        return left.major == right.major && left.minor == right.minor;
    }
    friend bool operator!=(const FileVersion& left, const FileVersion& right)
    {
        return !(left == right);
    }
};

}

namespace fmt{
template <>
struct formatter<phkvs::FileVersion> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) const { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const phkvs::FileVersion& ver, FormatContext &ctx) {
        return format_to(ctx.begin(), "v{}.{}", ver.major, ver.minor);
    }
};

}
