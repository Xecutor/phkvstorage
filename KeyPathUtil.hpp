#pragma once

#include <vector>
#include <tuple>
#include <stdexcept>

#include <fmt/format.h>
#include <boost/utility/string_view.hpp>

namespace phkvs {

inline std::vector<boost::string_view> splitDirPath(const boost::string_view& keyPath)
{
    std::vector<boost::string_view> result;
    std::string::size_type lastPos = !keyPath.empty() && keyPath[0] == '/' ? 1 : 0;
    do
    {
        auto pos = keyPath.find('/', lastPos);
        if(pos == std::string::npos)
        {
            if(lastPos != keyPath.length())
            {
                result.emplace_back(keyPath.data() + lastPos);
            }
            break;
        }
        if(pos - lastPos != 0)
        {
            result.emplace_back(keyPath.data() + lastPos, pos - lastPos);
        }
        lastPos = pos + 1;
    } while(lastPos < keyPath.length());
    return result;
}

struct PathAndKey{
    std::vector<boost::string_view> path;
    boost::string_view key;
};

inline PathAndKey splitKeyPath(const boost::string_view& keyPath)
{
    auto path = splitDirPath(keyPath);
    if(path.empty())
    {
        throw std::runtime_error(fmt::format("Invalid key path:{}", keyPath));
    }
    auto key = path.back();
    path.pop_back();
    return {std::move(path), key};
}

}
