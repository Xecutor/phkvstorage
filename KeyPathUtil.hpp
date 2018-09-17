#pragma once

#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/utility/string_view.hpp>

namespace phkvs{

std::vector<boost::string_view> splitKeyPath(const boost::string_view& keyPath)
{
    std::vector<boost::string_view> result;
    std::string::size_type lastPos = !keyPath.empty() && keyPath[0] == '/' ? 1 : 0;
    do
    {
        auto pos = keyPath.find('/', lastPos);
        if(pos==std::string::npos)
        {
            result.emplace_back(keyPath.data() + lastPos);
            break;
        }
        result.emplace_back(keyPath.data() + lastPos, pos - lastPos);
        lastPos = pos + 1;
    }while(lastPos<keyPath.length());
    return result;
}

}