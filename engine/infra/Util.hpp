#pragma once
//---------------------------------------------------------------------------
#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename T>
[[gnu::always_inline]] inline void unalignedStore(void* ptr, const T& value) noexcept {
    static_assert(std::is_trivially_copyable_v<T>);
    __builtin_memcpy(ptr, &value, sizeof(T));
}
//---------------------------------------------------------------------------
template <typename T>
[[gnu::always_inline]] inline T unalignedLoad(const void* ptr) noexcept {
    static_assert(std::is_trivially_copyable_v<T>);
    T value;
    __builtin_memcpy(&value, ptr, sizeof(T));
    return value;
}
//---------------------------------------------------------------------------
namespace detail {
template <typename T, typename Def>
struct IsFunT;

template <typename T, typename Dest>
struct IsFitFor {
    static constexpr bool value = std::is_void_v<Dest> || std::is_convertible_v<T, Dest>;
};

template <typename T, typename R, typename... Args>
struct IsFunT<T, R(Args...)> {
    private:
    template <typename U>
    static auto test(int) -> decltype(std::declval<U>()(std::declval<Args>()...), std::true_type{});

    template <typename>
    static std::false_type test(...);

    public:
    static constexpr bool value = decltype(test<T>(0))::value && IsFitFor<decltype(std::declval<T>()(std::declval<Args>()...)), R>::value;
};
}
//---------------------------------------------------------------------------
template <typename T, typename Def>
constexpr bool Fun = detail::IsFunT<T, Def>::value;
//---------------------------------------------------------------------------
template <typename T>
class FunctionRef;
template <typename R, typename... Args>
class FunctionRef<R(Args...)> {
    private:
    using Func = R (*)(const void*, Args...);
    Func func = nullptr;
    const void* obj = nullptr;

    public:
    constexpr FunctionRef() noexcept = default;
    template <typename F, typename = std::enable_if_t<Fun<F, R(Args...)>>>
    constexpr FunctionRef(const F& f) {
        obj = &f;
        func = [](const void* o, Args... args) -> R {
            return (*static_cast<const F*>(o))(std::forward<Args>(args)...);
        };
    }
    constexpr FunctionRef(R (*f)(Args...)) {
        obj = f;
        func = [](const void* o, Args... args) -> R {
            return static_cast<decltype(f)>(o)(std::forward<Args>(args)...);
        };
    }
    R operator()(Args... args) const { return func(obj, std::forward<Args>(args)...); }
    explicit operator bool() const noexcept { return func != nullptr; }
};
//---------------------------------------------------------------------------
// cacheline sizes for x64, ARM, and Power8
#if defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__)
static constexpr size_t hardwareCachelineSize = 64;
#elif defined(__PPC64__)
static constexpr size_t hardwareCachelineSize = 128;
#else
static constexpr size_t hardwareCachelineSize = 64;
#endif
//---------------------------------------------------------------------------
}
