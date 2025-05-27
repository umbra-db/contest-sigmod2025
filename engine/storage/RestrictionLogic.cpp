#include "storage/RestrictionLogic.hpp"
#include "infra/SmallVec.hpp"
#include "op/Hashtable.hpp"
#include "query/Restriction.hpp"
#include "storage/BitLogic.hpp"
#if defined(__x86_64__)
#include <immintrin.h>
#endif
#if defined(__x86_64__) && defined(__AVX512F__)
constexpr size_t vector_elements = 16;
#elif defined(__x86_64__) && defined(__AVX2__)
constexpr size_t vector_elements = 8;
#elif defined(__x86_64__) && defined(__AVX__)
constexpr size_t vector_elements = 4;
#elif defined(__aarch64__)
constexpr size_t vector_elements = 4;
#else
constexpr size_t vector_elements = 4;
#endif
typedef unsigned value_vector __attribute__((vector_size(vector_elements * sizeof(uint32_t))));
typedef bool mask_vector __attribute__((ext_vector_type(vector_elements)));
[[gnu::always_inline]] static inline value_vector load_unaligned(const uint32_t* ptr) {
    value_vector targetVec;
    __builtin_memcpy(&targetVec, ptr, sizeof(targetVec));
    return targetVec;
}
[[gnu::always_inline]] static inline value_vector broadcast(uint32_t value) {
    return value_vector{} + value;
}
template <typename T>
[[gnu::always_inline]] static inline uint64_t movemask(const T& vec) {
    mask_vector m = __builtin_convertvector(vec, mask_vector);
    uint64_t result = 0;
    __builtin_memcpy(&result, &m, sizeof(m));
    return result & ((1ull << vector_elements) - 1);
}
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename T>
struct RestrictionBuilder : public RestrictionLogic {
    /// Default
    static constexpr size_t vectorSize = 4;
    uint64_t runSparseImpl(const uint32_t* values, const uint8_t* src, const uint8_t* srcEnd) const {
        uint64_t newMask = 0;
        for (; src != srcEnd; ++src) {
            auto value = values[*src];
            bool match = static_cast<const T&>(*this).check(value);
            newMask |= static_cast<uint64_t>(match) << *src;
        }
        return newMask;
    }
    uint64_t runSparse(const uint32_t* values, uint64_t mask) const final {
        std::array<uint8_t, 71> offsets;
        uint8_t* offsetsEnd = BitLogic::bitsToOffsets(offsets.data(), mask);
        return static_cast<const T*>(this)->runSparseImpl(values, offsets.data(), offsetsEnd);
    }
    [[gnu::noinline]] uint64_t runDenseImpl(const uint32_t* values, size_t len) const {
        uint64_t newMask = 0;
        assert(len <= 64);

        auto* end = values + len;
        auto* vs = values;
        unsigned shift = 0;
        for (; vs < end - T::vectorSize + 1; vs += T::vectorSize, shift += T::vectorSize)
            newMask += static_cast<uint64_t>(static_cast<const T&>(*this).computeMask(vs)) << shift;
        for (; vs < end; ++vs, ++shift)
            newMask += static_cast<uint64_t>(static_cast<const T&>(*this).check(*vs)) << shift;

        return newMask;
    }
    [[gnu::always_inline]] uint64_t computeMask(const uint32_t* vs) const {
        auto v0 = *vs;
        auto v1 = *(vs + 1);
        auto v2 = *(vs + 2);
        auto v3 = *(vs + 3);

        bool m0 = static_cast<const T&>(*this).check(v0);
        bool m1 = static_cast<const T&>(*this).check(v1);
        bool m2 = static_cast<const T&>(*this).check(v2);
        bool m3 = static_cast<const T&>(*this).check(v3);

        uint64_t n0 = (m0 << 0) | (m1 << 1);
        uint64_t n1 = (m2 << 2) | (m3 << 3);

        return n0 | n1;
    }
    uint64_t runDense(const uint32_t* values, size_t len) const final {
        return static_cast<const T*>(this)->runDenseImpl(values, len);
    }
    std::pair<uint64_t, size_t> runAndSkip(const uint32_t* values, size_t len) const final {
        uint64_t mask = 0;
        auto* vs = values;
        auto* end = values + len;
        unsigned shift = 0;

        for (; vs < end - T::vectorSize + 1; vs += T::vectorSize) {
            mask = static_cast<const T&>(*this).computeMask(vs);
            if (mask) {
                shift = T::vectorSize;
                goto found;
            }
        }
        for (; vs < end; ++vs) {
            mask = static_cast<const T&>(*this).check(*vs);
            if (mask) {
                shift = 1;
                goto found;
            }
        }
        return {0, len};

    found:
        size_t skipped = vs - values;
        vs += shift;
        assert(skipped < len);
        end = std::min(end, vs + 64 - shift);
        for (; vs < end - T::vectorSize + 1; vs += T::vectorSize, shift += T::vectorSize)
            mask += static_cast<uint64_t>(static_cast<const T&>(*this).computeMask(vs)) << shift;

        for (; vs < end; ++vs, ++shift)
            mask += static_cast<uint64_t>(static_cast<const T&>(*this).check(*vs)) << shift;

        return {mask, skipped};
    }

    std::string_view name() const override {
        return ClassInfo::getName<T>();
    }
};
//---------------------------------------------------------------------------
struct EQRestriction : public RestrictionBuilder<EQRestriction> {
    uint32_t target;

    explicit EQRestriction(uint32_t a) : target(a) {}

    [[gnu::always_inline]] inline bool check(uint32_t v) const { return v == target; }
    static constexpr size_t vectorSize = vector_elements;
    [[gnu::always_inline]] inline uint64_t computeMask(const uint32_t* vs) const {
        value_vector v = load_unaligned(vs);
        auto cmp = v == broadcast(target);
        return movemask(cmp);
    }

    double estimateSelectivity() const final { return 0.01; }
    double estimateCost() const final { return 1; }
};
//---------------------------------------------------------------------------
struct NullRestriction final : public RestrictionBuilder<NullRestriction> {
    // This is only used for samples
    [[gnu::always_inline]] inline bool check(uint32_t v) const { return v != static_cast<uint32_t>(RuntimeValue::nullValue); }

    double estimateSelectivity() const final { return 0.9; }
    double estimateCost() const final { return 1; }
};
//---------------------------------------------------------------------------
static const NullRestriction nullRestrictionInstance;
//---------------------------------------------------------------------------
const RestrictionLogic* RestrictionLogic::notNullRestriction = &nullRestrictionInstance;
//---------------------------------------------------------------------------
struct EQ2Restriction : public RestrictionBuilder<EQ2Restriction> {
    std::array<uint32_t, 2> target{};

    explicit EQ2Restriction(uint32_t a, uint32_t b) : target({a, b}) {}

    [[gnu::always_inline]] inline bool check(uint32_t v) const {
        return (target[0] == v) | (target[1] == v);
    }
    static constexpr size_t vectorSize = vector_elements;
    [[gnu::always_inline]] inline uint64_t computeMask(const uint32_t* vs) const {
        auto v = load_unaligned(vs);
        auto cmp0 = v == broadcast(target[0]);
        auto cmp1 = v == broadcast(target[1]);
        auto cmp = cmp0 || cmp1;
        return movemask(cmp);
    }

    double estimateSelectivity() const { return 0.02; }
    double estimateCost() const final { return 1; }
};
//---------------------------------------------------------------------------
struct GtRestriction : public RestrictionBuilder<GtRestriction> {
    uint32_t target;

    explicit GtRestriction(uint32_t a) : target(a) {}

    [[gnu::always_inline]] inline bool check(uint32_t v) const {
        return target < v;
    }
    static constexpr size_t vectorSize = vector_elements;
    [[gnu::always_inline]] inline uint64_t computeMask(const uint32_t* vs) const {
        value_vector v = load_unaligned(vs);
        auto cmp = v > broadcast(target);
        return movemask(cmp);
    }

    double estimateSelectivity() const final { return double(std::numeric_limits<uint32_t>::max() - target) / std::numeric_limits<uint32_t>::max(); }
    double estimateCost() const final { return 1; }
};
//---------------------------------------------------------------------------
struct LtRestriction : public RestrictionBuilder<LtRestriction> {
    uint32_t target;

    explicit LtRestriction(uint32_t a) : target(a) {}

    [[gnu::always_inline]] inline bool check(uint32_t v) const {
        return v < target;
    }
    static constexpr size_t vectorSize = vector_elements;
    [[gnu::always_inline]] inline uint64_t computeMask(const uint32_t* vs) const {
        value_vector v = load_unaligned(vs);
        auto cmp = v < broadcast(target);
        return movemask(cmp);
    }

    double estimateSelectivity() const final { return double(target) / std::numeric_limits<uint32_t>::max(); }
    double estimateCost() const final { return 1; }
};
//---------------------------------------------------------------------------
struct BetweenRestriction : public RestrictionBuilder<BetweenRestriction> {
    std::array<uint32_t, 2> target{};

    explicit BetweenRestriction(uint32_t a, uint32_t b) : target({a, b}) {
        assert(a < b);
    }

    [[gnu::always_inline]] inline bool check(uint32_t v) const {
        return (target[0] < v) & (target[1] > v);
    }
    static constexpr size_t vectorSize = vector_elements;
    [[gnu::always_inline]] inline uint64_t computeMask(const uint32_t* vs) const {
        auto v = load_unaligned(vs);
        auto cmp0 = v > broadcast(target[0]);
        auto cmp1 = v < broadcast(target[1]);
        auto cmp = cmp0 && cmp1;
        return movemask(cmp);
    }

    double estimateSelectivity() const final { return double(target[1] - target[0]) / std::numeric_limits<uint32_t>::max(); }
    double estimateCost() const final { return 1; }
};
//---------------------------------------------------------------------------
struct JoinFilterRestriction : public RestrictionBuilder<JoinFilterRestriction> {
    const Hashtable* hashtable;

    double estimateSelectivity() const final { return 0.5; }
    double estimateCost() const final {
        return (hashtable->htSize() * sizeof(uint16_t) < 32*1024) ? 1.5 : 3;
    }

    explicit JoinFilterRestriction(const Hashtable* hashtable) : hashtable(hashtable) {}

    [[gnu::always_inline]] inline bool check(uint32_t v) const {
        return hashtable->joinFilter(v);
    }

#ifdef AVX_JOINFILTER
    [[gnu::always_inline]] inline uint64_t computeMask(const uint32_t* vs) const {
        const auto mul0 = broadcast(0x85ebca6b);
        const auto mul1 = broadcast(0xc2b2ae35);
        value_vector v = load_unaligned(vs);
        value_vector h1 = _mm512_mullo_epi32(v, mul0);
        h1 = h1 >> hashtable->shift;
        value_vector entry = _mm512_i32gather_epi32(h1, hashtable->bloom, 2);
        entry = entry << 16;

        value_vector h3 = _mm512_mullo_epi32(v, mul1);
        h3 = h3 >> (32 - 11);
        value_vector mask = _mm512_i32gather_epi32(h3, JoinFilter::bloomMasks, 2);
        mask = mask << 16;

        value_vector cmp = _mm512_andnot_epi32(entry, mask);
        cmp = cmp == 0;

        /*                auto newMask = movemask(cmp);

                int cmp_test = 0;
                for (unsigned i = 15  ; i < vector_elements; --i) {
                    auto test = vs[i];
                    auto h1_test = (test * 0x85ebca6b) >> hashtable->shift;
                    auto entry_test = hashtable->bloom[h1_test];

                    auto h3_test = (test * 0xc2b2ae35) >> (32 - 11);
                    auto mask_test = JoinFilter::bloomMasks[h3_test];
                    auto b = !(~entry_test & mask_test);
                    auto c = check(test);
                    if (b != c) {
                        assert(b == check(test));
                    }
                    assert(b == check(test));

                    cmp_test = cmp_test*2 + b;
                }
            assert(newMask == cmp_test);*/

        return movemask(cmp);
    }
#endif
};
//---------------------------------------------------------------------------
struct JoinFilterPreciseRestriction : public RestrictionBuilder<JoinFilterPreciseRestriction> {
    const Hashtable* hashtable;

    double estimateSelectivity() const final { return 0.5; }
    double estimateCost() const final {
        return (hashtable->htSize() * sizeof(uint16_t) < 32*1024) ? 1.5 : 3;
    }

    explicit JoinFilterPreciseRestriction(const Hashtable* hashtable) : hashtable(hashtable) {}

    [[gnu::always_inline]] inline bool check(uint32_t v) const {
        return hashtable->joinFilterPrecise(v);
    }
};
//---------------------------------------------------------------------------
template <size_t N, typename Hash>
struct TinyTable : public RestrictionBuilder<TinyTable<N, Hash>>, Hash {
    std::array<uint32_t, N> values{};

    double estimateSelectivity() const final { return 0.003; }
    double estimateCost() const final {
        return 1;
    }

    explicit TinyTable(Hash hasher) : Hash(hasher) {}

    [[gnu::always_inline]] inline bool check(uint32_t v) const {
        return values[Hash::operator()(v)] == v;
    }
};
//---------------------------------------------------------------------------
template <size_t N, typename Hash>
constexpr std::array<uint32_t, N> makeInvalidValues() {
    Hash hasher{};
    std::array<uint32_t, N> result{};
    // Try to set each value to something invalid
    for (size_t i = 0; i < N; i++)
        result[i] = ~0u;
    // But one slot is correctly set, switch that to something invalid as well
    auto slot = hasher(~0u) % N;
    for (uint32_t test : {0u, 1u, ~0u - 1u, 33u, 128u}) {
        if (hasher(test) != slot) {
            result[slot] = test;
            break;
        }
    }
    // Check that the slot is set
    assert(result[slot] != ~0u);
    return result;
}
//---------------------------------------------------------------------------
template <size_t N, typename Hash>
UniquePtr<TinyTable<N, Hash>> makeTinyTable(SmallVec<uint32_t, 32>& vals, Hash hasher) {
    // Try to make tiny lookup table. May fail!
    auto result = makeUnique<TinyTable<N, Hash>>(hasher);
    // Try to set each value to something invalid
    auto orgValues = makeInvalidValues<N, Hash>();
    result->values = orgValues;

    for (auto v : vals) {
        auto slot = hasher(v);
        // If we have a collision, give up
        if (result->values[slot] != orgValues[slot])
            return {};
        result->values[slot] = v;
    }
    return result;
}
//---------------------------------------------------------------------------
template <size_t N>
struct Identity {
    [[gnu::always_inline]] inline constexpr uint32_t operator()(uint32_t v) const { return v % N; }
};
//---------------------------------------------------------------------------
template <size_t N>
struct Shift {
    uint32_t amount = 0;
    [[gnu::always_inline]] inline constexpr uint32_t operator()(uint32_t v) const { return (v >> amount) % N; }
};
//---------------------------------------------------------------------------
template <size_t Bits>
struct Fibo {
    [[gnu::always_inline]] inline constexpr uint32_t operator()(uint32_t v) const {
        auto v2 = v * 11400714819323198485llu;
        return v2 >> (64 - Bits);
    }
};
//---------------------------------------------------------------------------
template <size_t Bits>
struct Fibo2 {
    [[gnu::always_inline]] inline constexpr uint32_t operator()(uint32_t v) const {
        auto v2 = v * 11400714819323198485llu;
        return static_cast<uint32_t>(v2 ^ (v2 >> 32)) >> (32 - Bits);
    }
};
//---------------------------------------------------------------------------
UniquePtr<RestrictionLogic> RestrictionLogic::setupRestriction(const Restriction& restriction) {
    if (restriction.type == Restriction::Type::Eq) {
        return makeUnique<EQRestriction>(restriction.cst.value);
    } else if ((restriction.type == Restriction::Type::Join) || (restriction.type == Restriction::Type::JoinPrecise)) {
        if (restriction.joinFilter->getNumTuples() <= 32) {
            // Collect the keys
            SmallVec<uint32_t, 32> keys;
            restriction.joinFilter->iterateAll([&](uint32_t key) {
                assert(keys.size() <= 32);
                keys.push_back(key);
            });
            std::sort(keys.begin(), keys.end());
            keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

            switch (keys.size()) {
                case 1: return makeUnique<EQRestriction>(keys[0]);
                case 2: return makeUnique<EQ2Restriction>(keys[0], keys[1]);
            }

            if (keys.back() - keys.front() == keys.size() - 1) {
                if (keys.front() == 0)
                    return makeUnique<LtRestriction>(keys.back() + 1);
                if (keys.back() == ~0u)
                    return makeUnique<GtRestriction>(keys.front() - 1);
                return makeUnique<BetweenRestriction>(keys.front() - 1, keys.back() + 1);
            }

            if (keys.size() <= 16) {
                if (auto res = makeTinyTable<8>(keys, Identity<8>{}))
                    return res;
                if (auto res = makeTinyTable<16>(keys, Identity<16>{}))
                    return res;
                if (auto res = makeTinyTable<32>(keys, Identity<32>{}))
                    return res;
                if (auto res = makeTinyTable<16>(keys, Fibo<4>{}))
                    return res;
                if (auto res = makeTinyTable<32>(keys, Fibo<5>{}))
                    return res;
                if (auto res = makeTinyTable<16>(keys, Fibo2<4>{}))
                    return res;
                if (auto res = makeTinyTable<32>(keys, Fibo2<5>{}))
                    return res;
            }
        }
        if (restriction.type == Restriction::Type::JoinPrecise)
            return makeUnique<JoinFilterPreciseRestriction>(restriction.joinFilter);
        else
            return makeUnique<JoinFilterRestriction>(restriction.joinFilter);
    }
    return {};
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
