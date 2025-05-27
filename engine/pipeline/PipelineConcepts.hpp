#pragma once
//---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>
#include <tuple>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class TargetBase;
class ScanBase;
//---------------------------------------------------------------------------
namespace helper {
//---------------------------------------------------------------------------
struct TestConsumer {
    template <typename T>
    void operator()(T provider) {}
};
//---------------------------------------------------------------------------
template <typename LocalState>
struct TestInitialConsumer {
    template <typename T>
    void operator()(LocalState& ls, T provider) {}
};
//---------------------------------------------------------------------------
struct TestProvider {
    uint64_t operator()(unsigned offset) const { return offset; }
};
//---------------------------------------------------------------------------
template <typename T1, typename T2>
using TupleCat = decltype(std::tuple_cat(std::declval<T1>(), std::declval<T2>()));
//---------------------------------------------------------------------------
/// A tuple of N unsigned
template <size_t N>
struct RepeatT {
    using type = TupleCat<typename RepeatT<N / 2>::type, typename RepeatT<(N + 1) / 2>::type>;
};
//---------------------------------------------------------------------------
template <>
struct RepeatT<0> {
    using type = std::tuple<>;
};
//---------------------------------------------------------------------------
template <>
struct RepeatT<1> {
    using type = std::tuple<uint64_t>;
};
//---------------------------------------------------------------------------
/// Invocable by tuple
template <typename T, typename Args>
struct InvocableByTupleT;
//---------------------------------------------------------------------------
template <typename T, typename... Args>
struct InvocableByTupleT<T, std::tuple<Args...>> {
    static constexpr bool value = std::is_invocable_v<T, typename T::LocalState&, Args...>;
};
//---------------------------------------------------------------------------
template <typename T>
struct is_probe_operator {
    private:
    template <typename U>
    static auto test(int) -> decltype(std::declval<U>()(std::declval<typename U::LocalState&>(), std::declval<uint32_t>(), std::declval<helper::TestConsumer>()), std::true_type{});

    template <typename>
    static std::false_type test(...);

    public:
    static constexpr bool value = decltype(test<T>(0))::value;
};
//---------------------------------------------------------------------------
template <typename T, size_t NumArguments>
struct is_target_operator {
    private:
    template <typename U>
    static auto test(int) -> decltype(
        helper::InvocableByTupleT<U, typename helper::RepeatT<NumArguments>::type>::value &&
        std::is_base_of<TargetBase, std::decay_t<U>>::value,
        std::true_type{}
    );

    template <typename>
    static std::false_type test(...);

    public:
    static constexpr bool value = decltype(test<T>(0))::value;
};
//---------------------------------------------------------------------------
template <typename T>
struct is_provider {
    private:
    template <typename U>
    static auto test(int) -> decltype(std::declval<U>()(std::declval<unsigned>()), std::true_type{});

    template <typename>
    static std::false_type test(...);

    public:
    static constexpr bool value = decltype(test<T>(0))::value;
};
//---------------------------------------------------------------------------
template <typename T>
struct is_consumer {
    private:
    template <typename U>
    static auto test(int) -> decltype(std::declval<U>()(std::declval<helper::TestProvider>()), std::true_type{});

    template <typename>
    static std::false_type test(...);

    public:
    static constexpr bool value = decltype(test<T>(0))::value;
};
//---------------------------------------------------------------------------
template <typename T, typename LocalState>
struct is_initial_consumer {
    private:
    template <typename U>
    static auto test(int) -> decltype(std::declval<U>()(std::declval<LocalState&>(), std::declval<helper::TestProvider>()), std::true_type{});

    template <typename>
    static std::false_type test(...);

    public:
    static constexpr bool value = decltype(test<T>(0))::value;
};
//---------------------------------------------------------------------------
template <typename T>
struct is_scan_operator {
    private:
/*    template <typename U>
    static auto test(int) -> decltype(std::declval<U&>()(std::declval<char*>(), std::declval<helper::TestInitialConsumer<char>>()), std::true_type{});

    template <typename>
    static std::false_type test(...);

    public:
    static constexpr bool value = decltype(test<T>(0))::value && std::is_base_of<ScanBase, std::decay_t<T>>::value;*/
    public:
    static constexpr  bool value = true;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
/// A probe operator is a functor that takes a key and a consumer and calls the consumer for each tuple with that key.
template <typename T>
constexpr bool ProbeOperator = helper::is_probe_operator<T>::value;
//---------------------------------------------------------------------------
/// A result target is a functor that takes a number of attributes and consumes them.
template <typename T, size_t NumArguments>
constexpr bool TargetOperator = helper::is_target_operator<T, NumArguments>::value;
//---------------------------------------------------------------------------
/// An iu provider is a functor that represents a relation. It takes an offset/index/key and returns a value
template <typename T>
constexpr bool Provider = helper::is_provider<T>::value;
//---------------------------------------------------------------------------
/// A consumer is a functor that consumes individual tuples given a provider for attributes from a relation
template <typename T>
constexpr bool Consumer = helper::is_consumer<T>::value;
//---------------------------------------------------------------------------
/// A consumer is a functor that consumes individual tuples given a provider for attributes from a relation
template <typename T, typename LocalState>
constexpr bool InitialConsumer = helper::is_initial_consumer<T, LocalState>::value;
//---------------------------------------------------------------------------
/// A scan is a function that calls a consumer for each tuple in a relation
template <typename T>
constexpr bool ScanOperator = helper::is_scan_operator<T>::value;
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
