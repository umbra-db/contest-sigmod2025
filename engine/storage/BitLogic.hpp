#pragma once
//---------------------------------------------------------------------------
#include "infra/helper/BitOps.hpp"
#include <cassert>
#include <cstdint>
#include <cstddef>
#include <utility>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class BitLogic {
    public:
    /// Convert the bits in a 64bit integer to an array of offsets (and the length of this array)
    /// Target should have size at least 64+7=71 bytes
    static uint8_t* bitsToOffsets(uint8_t* target, uint64_t bits) noexcept;

    /// Is a bitset dense? Are all bits packed together?
    static constexpr bool isDense(uint64_t mask) {
        auto lowest = mask & -mask;
        auto incremented = mask + lowest;
        return (incremented & (incremented - 1)) == 0;
    }
    /// Get the range of bits from a bitset (assuming it is dense)
    static constexpr std::pair<size_t, size_t> getRange(uint64_t mask) {
        assert(isDense(mask));
        if (!mask)
            return {0, 0};
        return {engine::countr_zero(mask), 64 - engine::countl_zero(mask)};
    }

    struct IndexIterator {
        size_t index = 0;

        void operator++() { index++; }
        bool operator!=(const IndexIterator& other) const { return index != other.index; }
        size_t operator*() const { return index; }
    };

    struct IndirectIterator {
        uint8_t* indices = nullptr;

        void operator++() { indices++; }
        bool operator!=(const IndirectIterator& other) const { return indices != other.indices; }
        size_t operator*() const { return *indices; }
    };


    /// Get the range of bits from a bitset (assuming it is dense)
    static std::pair<IndexIterator, IndexIterator> getDenseIterators(uint64_t mask) {
        assert(isDense(mask));
        if (!mask)
            return {IndexIterator{0}, IndexIterator{0}};
        return {IndexIterator{size_t(engine::countr_zero(mask))}, IndexIterator{size_t(64 - engine::countl_zero(mask))}};
    }
    /// Get the range of bits from a bitset (assuming it is dense)
    static std::pair<IndirectIterator, IndirectIterator> getSparseIterators(uint8_t* target, uint64_t mask) {
        if (!mask)
            return {IndirectIterator{nullptr}, IndirectIterator{nullptr}};
        auto* end = bitsToOffsets(target, mask);
        return {IndirectIterator{target}, IndirectIterator{end}};
    }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------