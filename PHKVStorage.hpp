#pragma once

#include <boost/variant.hpp>
#include <boost/optional.hpp>

namespace phkvs{

class PHKVStorage{
public:
    using ValueType = boost::variant<uint8_t, uint16_t, uint32_t, uint64_t,
        float, double, std::string, std::vector<uint8_t>>;

    virtual void store(const std::string& keyPath, const ValueType& value) = 0;

    virtual boost::optional<ValueType> retrieve(const std::string& keyPath) = 0;

private:
};

}
