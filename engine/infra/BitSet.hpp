#pragma once
//---------------------------------------------------------------------------
#include "infra/helper/BitOps.hpp"
#include <bit>
#include <cassert>
#include <cstdint>
#include <iterator>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename BS>
class BitSetIterator {
    BS set;

    public:
    constexpr BitSetIterator(BS set) noexcept : set(set) {}
    constexpr BitSetIterator() noexcept = default;
    using iterator_category = std::forward_iterator_tag;
    using value_type = unsigned;
    using difference_type = ptrdiff_t;

    constexpr unsigned operator*() const noexcept { return set.front(); }
    constexpr BitSetIterator& operator++() noexcept {
        set.pop_front();
        return *this;
    }
    constexpr auto operator++(int) noexcept {
        auto rv = *this;
        operator++();
        return rv;
    }

    bool operator==(const BitSetIterator& o) const noexcept { return set == o.set; }
    bool operator!=(const BitSetIterator& o) const noexcept { return set != o.set; }
};
//---------------------------------------------------------------------------
template <typename BS>
class BitSetReverseIterator {
    BS set;

    public:
    constexpr BitSetReverseIterator(BS set) noexcept : set(set) {}
    constexpr BitSetReverseIterator() noexcept = default;
    using iterator_category = std::forward_iterator_tag;
    using value_type = unsigned;
    using difference_type = ptrdiff_t;

    constexpr unsigned operator*() const noexcept { return set.back(); }
    constexpr BitSetReverseIterator& operator++() noexcept {
        set.pop_back();
        return *this;
    }
    constexpr auto operator++(int) noexcept {
        auto rv = *this;
        operator++();
        return rv;
    }

    bool operator==(const BitSetReverseIterator& o) const noexcept { return set == o.set; }
    bool operator!=(const BitSetReverseIterator& o) const noexcept { return set != o.set; }
};
//---------------------------------------------------------------------------
template <typename BS>
class BitSetReverseAdapter {
    const BS* set = nullptr;

    public:
    constexpr BitSetReverseAdapter(const BS& set) noexcept : set(&set) {}

    using iterator = BitSetReverseIterator<BS>;

    constexpr iterator begin() const noexcept { return iterator{*set}; }
    constexpr iterator end() const noexcept { return iterator{}; }
};
//---------------------------------------------------------------------------
template <typename BS>
class BitSetSubsetsAdapter {
    const BS* set = nullptr;

    public:
    constexpr BitSetSubsetsAdapter(const BS& set) noexcept : set(&set) {}

    class iterator {
        BS current;
        const BS* mask = nullptr;

        template <typename BS2>
        friend class BitSetSubsetsAdapter;

        constexpr iterator(BS current, const BS& mask) noexcept : current(std::move(current)), mask(&mask) {}

        public:
        constexpr iterator() noexcept = default;
        using iterator_category = std::forward_iterator_tag;
        using value_type = BS;
        using difference_type = ptrdiff_t;

        constexpr BS operator*() const noexcept { return current; }
        constexpr iterator& operator++() noexcept {
            assert(mask);
            current.increment(*mask);
            return *this;
        }
        constexpr auto operator++(int) noexcept {
            auto rv = *this;
            operator++();
            return rv;
        }

        constexpr bool operator==(const iterator& o) const noexcept { return current == o.current; }
        constexpr bool operator!=(const iterator& o) const noexcept { return current != o.current; }
    };

    constexpr iterator begin() const noexcept { return iterator((set->empty() ? BS{} : BS{set->front()}), *set); }
    constexpr iterator end() const noexcept { return iterator{}; }
};
//---------------------------------------------------------------------------
template <typename Underlying>
class BitSetT {
    Underlying raw = 0;

    constexpr BitSetT(Underlying raw) noexcept : raw(raw) {}

    public:
    using arg_type = BitSetT;
    constexpr BitSetT() noexcept = default;
    constexpr BitSetT(std::initializer_list<unsigned> lst) noexcept {
        for (auto i : lst)
            insert(i);
    }
    template<typename IteratorType>
    constexpr BitSetT(IteratorType&& begin, IteratorType&& end) noexcept {
        for (auto it = begin; it != end; ++it)
            insert(*it);
    }

    constexpr void insert(unsigned i) noexcept { raw |= (1ull << i); }
    constexpr void erase(unsigned i) noexcept { raw &= ~(1ull << i); }
    constexpr bool contains(unsigned i) const noexcept { return raw & (1ull << i); }
    constexpr void clear() noexcept { raw = 0; }

    constexpr unsigned front() const noexcept {
        assert(!empty());
        return engine::countr_zero(raw);
    }
    constexpr BitSetT frontSet() const noexcept {
        assert(!empty());
        return BitSetT(raw & -raw);
    }
    constexpr unsigned back() const noexcept { return sizeof(raw) * 8 - 1 - engine::countl_zero(raw); }
    constexpr void pop_back() noexcept { erase(back()); }
    constexpr void pop_front() noexcept { raw &= (raw - 1); }
    constexpr void increment(arg_type mask) noexcept {
        // Add the mask's two's complement to raw
        raw = mask.raw & (raw - mask.raw);
    }
    constexpr bool single() const noexcept { return engine::has_single_bit(raw); }
    constexpr bool singleNonEmpty() const noexcept {
        assert(!empty());
        return (raw & (raw - 1)) == 0;
    }
    constexpr unsigned size() const noexcept { return engine::popcount(raw); }
    constexpr bool empty() const noexcept { return raw == 0; }

    /// Get the index of set bit among all bits
    constexpr unsigned getIndex(unsigned b) const noexcept {
        assert(contains(b));
        return (*this & prefix(b)).size();
    };

    constexpr bool operator==(arg_type o) const noexcept { return raw == o.raw; }
    constexpr bool operator!=(arg_type o) const noexcept { return raw != o.raw; }

    constexpr BitSetT& operator+=(arg_type o) noexcept {
        raw |= o.raw;
        return *this;
    }
    constexpr BitSetT& operator-=(arg_type o) noexcept {
        raw &= ~o.raw;
        return *this;
    }
    constexpr BitSetT& operator&=(arg_type o) noexcept {
        raw &= o.raw;
        return *this;
    }

    constexpr bool isSubsetOf(arg_type o) const noexcept { return (raw & ~o.raw) == 0; }
    constexpr bool intersectsWith(arg_type o) const noexcept { return (raw & o.raw) != 0; }

    constexpr BitSetT operator+(arg_type o) const noexcept {
        BitSetT rv = *this;
        rv += o;
        return rv;
    }
    constexpr BitSetT operator-(arg_type o) const noexcept {
        BitSetT rv = *this;
        rv -= o;
        return rv;
    }
    constexpr BitSetT operator&(arg_type o) const noexcept {
        BitSetT rv = *this;
        rv &= o;
        return rv;
    }

    static constexpr BitSetT prefix(unsigned i) noexcept { return BitSetT((1ull << i) - 1); }

    using iterator = BitSetIterator<BitSetT>;
    using reverse_iterator = BitSetReverseIterator<BitSetT>;
    using SubsetsAdapter = BitSetSubsetsAdapter<BitSetT>;
    using ReverseAdapter = BitSetReverseAdapter<BitSetT>;

    constexpr iterator begin() const noexcept { return iterator(*this); }
    constexpr iterator end() const noexcept { return iterator{}; }
    constexpr reverse_iterator rbegin() const noexcept { return reverse_iterator(*this); }
    constexpr reverse_iterator rend() const noexcept { return reverse_iterator{}; }
    SubsetsAdapter subsets() const noexcept { return SubsetsAdapter{*this}; }
    ReverseAdapter reversed() const noexcept { return ReverseAdapter{*this}; }

    constexpr uint64_t asU64() const noexcept { return raw; }
    static constexpr BitSetT fromU64(uint64_t raw) noexcept { return BitSetT(raw); }

    /// Hash function
    struct Hash {
        constexpr uint64_t operator()(BitSetT set) const noexcept { return std::hash<uint64_t>{}(set.raw); }
    };

    // auto operator<=>(const BitSetT&) const noexcept = default;
    constexpr bool operator<(const BitSetT& other) const noexcept { return raw < other.raw; }
    constexpr bool operator>(const BitSetT& other) const noexcept { return raw < other.raw; }
    constexpr bool operator<=(const BitSetT& other) const noexcept { return raw <= other.raw; }
    constexpr bool operator>=(const BitSetT& other) const noexcept { return raw >= other.raw; }
};
//---------------------------------------------------------------------------
using BitSet = BitSetT<uint64_t>;
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------