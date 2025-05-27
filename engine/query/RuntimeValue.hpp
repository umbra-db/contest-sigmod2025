#pragma once
//---------------------------------------------------------------------------
#include <cstdint>
#include <memory>
#include <attribute.h>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct RuntimeValue {
    static constexpr uint64_t nullValue = std::numeric_limits<uint64_t>::max();
    DataType type;
    uint64_t value;

    static RuntimeValue from(DataType type, uint64_t value) {
        return {type, value};
    }

    /// Is null?
    constexpr bool isNull() const {
        return value == nullValue;
    }
    /// Compare
    bool operator==(const RuntimeValue& other) const {
        return type == other.type && value == other.value;
    }
    /// Compare
    bool operator!=(const RuntimeValue& other) const {
        return !operator==(other);
    }
};
//---------------------------------------------------------------------------
}
