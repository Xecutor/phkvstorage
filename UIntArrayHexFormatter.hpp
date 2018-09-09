#pragma once

#include <fmt/format.h>
#include <array>
#include <type_traits>

namespace fmt {

template<class T, size_t N>
struct formatter<std::array<T, N>> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template<typename FormatContext, typename = std::enable_if_t<std::is_integral_v<T>>>
    auto format(const std::array<T, N>& arr, FormatContext& ctx)
    {
        auto fmt = format_to(ctx.begin(), "[");
        bool first = true;
        for(auto& val : arr)
        {
            if(first)
            {
                fmt = format_to(fmt, "{1:0{0}x}", sizeof(val) * 2, val);
                first = false;
            } else
            {
                fmt = format_to(fmt, ", {1:0{0}x}", sizeof(val) * 2, val);
            }
        }
        return format_to(fmt, "]");
    }
};

}