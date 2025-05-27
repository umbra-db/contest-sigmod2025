#pragma once
//---------------------------------------------------------------------------
#include <array>
#include <string_view>
#include <type_traits>
#include <utility>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// Enum helpers
struct EnumInfo {
    private:
    /// Get the name of an enum value
    template <auto E>
    static constexpr std::string_view getNameImpl() {
        std::string_view res(__PRETTY_FUNCTION__);
        size_t pos = res.find("E = ");
        if (pos == std::string_view::npos)
            return "";
        pos += 4;

        auto isName = [](char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
        };
        auto cur = pos;
        while (cur < res.size() && (isName(res[cur]) || (res[cur] == ':')))
            cur++;
        auto end = cur;
        // Go back until pos to find :
        while (cur >= pos && res[cur] != ':')
            cur--;
        return res.substr(cur + 1, end - cur - 1);
    }
    /// Get the maximum value for enum
    template <typename T, size_t E>
    static constexpr size_t getSizeImpl() {
        if constexpr (getNameImpl<static_cast<T>(E)>().empty())
            return E;
        else
            return getSizeImpl<T, E + 1>();
    }
    /// Get the number of valid elements in a standard enum
    template <typename T>
    static constexpr size_t getSize() {
        return getSizeImpl<T, 0>();
    }

    template<typename T, size_t... I>
    static constexpr size_t getNamesLengthImpl(std::index_sequence<I...>) {
        return (getNameImpl<static_cast<T>(I)>().size() + ...);
    }

    /// Get the total length of the enum names
    template <typename T>
    static constexpr size_t getNamesLength() {
        return getNamesLengthImpl<T>(std::make_index_sequence<size<T>>{});
    }

    template<typename T, size_t I>
    static void constexpr getNamesSingle(unsigned& offset, std::array<char, getNamesLength<T>()>& res, std::integral_constant<size_t, I>){
        auto name = getNameImpl<static_cast<T>(I)>();
        name.copy(res.data() + offset, name.size());
        offset += name.size();
    }
    template <typename T, size_t I>
    static void constexpr getSVsSingle(unsigned& offset, std::array<char, getNamesLength<T>()>& res, std::integral_constant<size_t, I>) {
        auto sz = getNameImpl<static_cast<T>(I)>().size();
        res[I] = std::string_view(names<T>.data() + offset, sz);
        offset += sz;
    }
    template<typename func, typename T, size_t... I>
    static auto applySingle(std::index_sequence<I...>) {
        std::array<std::string_view, size<T>> res{};
        unsigned offset = 0;
        (func(offset, res, std::integral_constant<size_t, I>{}), ...);
        return res;
    }

    /// Get the name contents of an enum
    template <typename T>
    static auto constexpr getNames() {
        return applySingle<getNamesSingle, T>(std::make_index_sequence<size<T>>{});
    }
    /// The names for an enum type
    template <typename T>
    static constexpr auto names = getNames<T>();

    /// Get the string_views
    template <typename T>
    static auto constexpr getSVs() {
        return applySingle<getSVsSingle, T>(std::make_index_sequence<size<T>>{});
    }
    /// The svs for an enum type
    template <typename T>
    static constexpr auto svs = getSVs<T>();

    public:
    /// Get the size of an enum
    template <typename T, typename = std::enable_if_t<std::is_enum_v<T>>>
    static constexpr size_t size = getSize<T>();
    /// Get the name of an enum value
    template <typename T, typename = std::enable_if<std::is_enum_v<T>>>
    static constexpr std::string_view getName(T value) {
        return svs<T>[static_cast<size_t>(value)];
    }
};
//---------------------------------------------------------------------------
struct ClassInfo {
    public:
    /// Get the name of a class
    template <typename T>
    static constexpr std::string_view getName() {
        std::string_view res(__PRETTY_FUNCTION__);
        size_t pos = res.find("T = ");
        if (pos == std::string_view::npos)
            return "";
        pos += 4;

        auto isName = [](char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':';
        };
        auto cur = pos;
        while (cur < res.size() && isName(res[cur]))
            cur++;
        return res.substr(pos, cur - pos);
    }
};
//---------------------------------------------------------------------------
}