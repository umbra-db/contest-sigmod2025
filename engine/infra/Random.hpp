#pragma once
//---------------------------------------------------------------------------
#include <cstdint>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct Random {
    static constexpr uint64_t addConstant = 0x2d358dccaa6c78a5ull;
    static constexpr uint64_t xorConstant = 0x8bb84b93962eacc9ull;
    uint64_t seed = 0;

    static uint64_t mix(uint64_t a, uint64_t b) {
        auto res = static_cast<unsigned __int128>(a) * b;
        return static_cast<uint64_t>(res >> 64) ^ static_cast<uint64_t>(res);
    }

    uint64_t operator()() {
        seed += addConstant;
        return mix(seed, seed ^ xorConstant);
    }

    constexpr explicit Random(uint64_t s = 0) : seed(mix(s, 0x8bb84b93962eacc9ull)) {}

    // Generate in range [0, s)
    uint64_t nextRange(uint64_t s) {
        uint64_t val = operator()();
        return (static_cast<unsigned __int128>(val) * s) >> 64;
    }
};
//---------------------------------------------------------------------------
}