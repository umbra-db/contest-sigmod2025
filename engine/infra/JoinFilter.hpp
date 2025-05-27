#pragma once
//---------------------------------------------------------------------------
#include <cstdint>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct JoinFilter {
    /// Lut for popcount == 4
    alignas(4096) static const uint16_t bloomMasks[2048];

    template <typename T>
    [[gnu::always_inline]] static inline uint16_t getMask(T hash) {
        return bloomMasks[hash >> (sizeof(hash) * 8 - 11)];
    }

    [[gnu::always_inline]] static inline bool checkMaskWithEntry(uint16_t mask, uint16_t entry) {
        return !(~entry & mask);
    }

    template<typename T>
    [[gnu::always_inline]] static inline bool checkEntry(T hash, uint16_t entry) {
        return checkMaskWithEntry(getMask(hash), entry);
    }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
