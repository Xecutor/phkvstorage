#pragma once

#include <fmt/format.h>
#include <boost/utility/string_view.hpp>

namespace fmt {

template<>
struct formatter<boost::string_view> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) const
    {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const boost::string_view& strView, FormatContext& ctx)
    {
        return format_to(ctx.begin(), "{:.{}}", strView.data(), strView.length());
    }
};

}