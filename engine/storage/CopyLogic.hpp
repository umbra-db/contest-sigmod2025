#pragma once
//---------------------------------------------------------------------------
#include <cstdint>
#include <cstddef>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class CopyLogic {
    public:
    /// Copy 32 bit integers with mask
    /// srcOffsets is a bitset that describes the indices of the src array that should be copied
    /// dstOffsets is a bitset that describes the indices of the dst array that should be copied to
    /// The rest of the indices of the dst array (up to numTuples) should be set to null (~0ull)
    static void extractInt32(uint64_t* dst, const uint32_t* src, uint64_t srcOffsets, uint64_t dstOffsets, size_t numTuples) noexcept;
    /// Copy 32 bit integers with mask
    /// srcOffsets is a bitset that describes the indices of the src array that should be copied
    /// dstOffsets is a bitset that describes the indices of the dst array that should be copied to
    /// The rest of the indices of the dst array (up to numTuples) should be set to null (~0ull)
    static void extractInt64(uint64_t* dst, const uint64_t* src, uint64_t srcOffsets, uint64_t dstOffsets, size_t numTuples) noexcept;
    /// Copy 32 bit strings with mask
    /// srcOffsets is a bitset that describes the indices of the src array that should be copied
    /// dstOffsets is a bitset that describes the indices of the dst array that should be copied to
    /// The rest of the indices of the dst array (up to numTuples) should be set to null (~0ull)
    /// All the strings are short strings
    static void extractVarChar(uint64_t* dst, const uint16_t* src, uint64_t srcOffsets, uint64_t dstOffsets, size_t numTuples, const char* stringHead) noexcept;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------