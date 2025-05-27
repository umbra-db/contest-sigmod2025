#pragma once
#include <bit>
#include <limits>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
constexpr auto Nd_ull = std::numeric_limits<unsigned long long>::digits;
constexpr auto Nd_ul = std::numeric_limits<unsigned long>::digits;
constexpr auto Nd_u =  std::numeric_limits<unsigned>::digits;
//---------------------------------------------------------------------------
[[noreturn]] inline void unreachable()
{
    // Uses compiler specific extensions if possible.
    // Even if no extension is used, undefined behavior is still raised by
    // an empty function body and the noreturn attribute.
#if defined(_MSC_VER) && !defined(__clang__) // MSVC
    __assume(false);
#else // GCC, Clang
    __builtin_unreachable();
#endif
}
//---------------------------------------------------------------------------
template <typename T>
constexpr int popcount(T x) noexcept {
    constexpr auto Nd = std::numeric_limits<T>::digits;

    if constexpr (Nd <= Nd_u)
        return __builtin_popcount(x);
    else if constexpr (Nd <= Nd_ul)
        return __builtin_popcountl(x);
    else if constexpr (Nd <= Nd_ull)
        return __builtin_popcountll(x);
    else
        unreachable();
}
//---------------------------------------------------------------------------
template <typename T>
constexpr int countr_zero(T x) noexcept {
    constexpr auto Nd = std::numeric_limits<T>::digits;

    if (x == 0)
        return Nd;

    if constexpr (Nd <= Nd_u)
        return __builtin_ctz(x);
    else if constexpr (Nd <= Nd_ul)
        return __builtin_ctzl(x);
    else if constexpr (Nd <= Nd_ull)
        return __builtin_ctzll(x);
    else
        unreachable();
}
//---------------------------------------------------------------------------
template <typename T>
constexpr int countl_zero(T x) noexcept {
    constexpr auto Nd = std::numeric_limits<T>::digits;

    if (x == 0)
        return Nd;

    if constexpr (Nd <= Nd_u) {
        constexpr int diff = Nd_u - Nd;
        return __builtin_clz(x) - diff;
    } else if constexpr (Nd <= Nd_ul) {
        constexpr int diff = Nd_ul - Nd;
        return __builtin_clzl(x) - diff;
    } else if constexpr (Nd <= Nd_ull) {
        constexpr int diff = Nd_ull - Nd;
        return __builtin_clzll(x) - diff;
    } else {
        unreachable();
    }
}
//---------------------------------------------------------------------------
template <typename T>
constexpr int has_single_bit(T x) noexcept {
    return popcount(x) == 1;
}
//---------------------------------------------------------------------------
template <typename T>
constexpr int bit_width(T x) noexcept {
    constexpr auto Nd = std::numeric_limits<T>::digits;
    return Nd - countl_zero(x);
}
//---------------------------------------------------------------------------
template <typename T>
constexpr int countr_one(T x) noexcept {
    return countr_zero((T)~x);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
