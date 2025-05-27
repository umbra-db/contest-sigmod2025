#pragma once
//---------------------------------------------------------------------------
#include "query/RuntimeValue.hpp"
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class Hashtable;
//---------------------------------------------------------------------------
/// A restriction
struct Restriction {
    static constexpr uint64_t nullValue = RuntimeValue::nullValue;
    /// The type order is used for ordering restrictions
    /// Selective & cheap comes first
    enum Type {
        /// Attribute is equal to value
        Eq,
        /// Attribute is not null
        NotNull,
        /// Attribute will likely find a join partner
        Join,
        /// Attribute will definitely find a join partner
        JoinPrecise
    };
    /// The type of the restriction
    Type type;
    /// The constant value compared with
    RuntimeValue cst;
    /// The hash table for join filters
    Hashtable* joinFilter;
    /// The selectivity estimation for the restriction
    double selectivity = 1.0;

    /// Check whether the restriction is satisfied by a value
    bool operator()(uint64_t val) const noexcept;
};
//---------------------------------------------------------------------------
}