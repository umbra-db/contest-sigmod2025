#pragma once
//---------------------------------------------------------------------------
#include "infra/BitSet.hpp"
#include "infra/Util.hpp"
#include <concepts>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename T>
struct QG {
    static_assert(std::is_same<decltype(std::declval<const T>().neighborhood(std::declval<typename BitSet::arg_type>())), BitSet>::value);
    static_assert(std::is_same<decltype(std::declval<const T>().connected(std::declval<typename BitSet::arg_type>())), bool>::value);
    static_assert(std::is_convertible<decltype(std::declval<const T>().size()), unsigned>::value);
};
//---------------------------------------------------------------------------
class DPccp {
    public:
    template <typename QG, typename Callback, typename = std::enable_if_t<Fun<Callback, void(BitSet::arg_type)>>>
    static void enumerateCsg(const QG& qg, unsigned N, Callback&& callback) {
        enumerateCsg(qg, BitSet::prefix(N), BitSet{}, callback);
    }

    template <typename QG, typename Callback, typename = std::enable_if_t<Fun<Callback, void(BitSet::arg_type)>>>
    static void enumerateCsg(const QG& qg, BitSet::arg_type s, BitSet::arg_type x, Callback&& callback) {
        for (unsigned i : s.reversed())
            enumerateCsgRec(qg, {i}, x + (BitSet::prefix(i) & s), callback);
    }

    template <typename QG, typename Callback, typename = std::enable_if_t<Fun<Callback, void(BitSet::arg_type)>>>
    static void enumerateCsgRec(const QG& qg, BitSet::arg_type s, BitSet::arg_type x, Callback&& callback) {
        callback(s);
        auto n = qg.neighborhood(s) - x;
        for (BitSet sp : n.subsets())
            enumerateCsgRec(qg, s + sp, x + n, callback);
    }

    template <typename QG, typename Callback, typename = std::enable_if_t<Fun<Callback, void(BitSet::arg_type)>>>
    static void enumerateCmp(const QG& qg, BitSet::arg_type s, Callback&& callback) {
        auto x = BitSet::prefix(s.front()) + s;
        auto n = qg.neighborhood(s) - x;
        enumerateCsg(qg, n, x, callback);
    }

    template <typename QG, typename Callback, typename = std::enable_if_t<Fun<Callback, void(BitSet::arg_type, BitSet::arg_type)>>>
    static void enumerateCsgCmp(const QG& qg,Callback && callback) {
        enumerateCsg(qg, qg.size(), [&](BitSet::arg_type s) {
            if (!qg.connected(s))
                return;
            enumerateCmp(qg, s, [&](BitSet::arg_type c) {
                if (!qg.connected(c))
                    return;
                callback(s, c);
            });
        });
    }
};
//---------------------------------------------------------------------------
}