#pragma once

#include <array>
#include <stdint.h>

#include "FileVersion.hpp"

namespace phkvs {

struct FileMagic{
    using Type = std::array<uint8_t,4>;
    Type magic{};

    FileMagic()=default;
    FileMagic(const Type& argMagic):magic(argMagic){}

    static constexpr size_t binSize()
    {
        return sizeof(magic);
    }

    void serialize(OutputBinBuffer& out)const
    {
        out.writeArray(magic);
    }

    void deserialize(InputBinBuffer& in)
    {
        in.readArray(magic);
    }

    bool operator==(const FileMagic& other)const
    {
        return magic == other.magic;
    }
    bool operator!=(const FileMagic& other)const
    {
        return magic != other.magic;
    }
    bool operator==(const FileMagic::Type& other)const
    {
        return magic == other;
    }
    bool operator!=(const FileMagic::Type& other)const
    {
        return magic != other;
    }
};

}

namespace fmt{
template <>
struct formatter<phkvs::FileMagic> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) const { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const phkvs::FileMagic& fileMagic, FormatContext &ctx) {
        return format_to(ctx.begin(), "{}", fileMagic.magic);
    }
};

}
