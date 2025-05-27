#pragma once
//---------------------------------------------------------------------------
#include "Config.hpp"
#include "infra/JoinFilter.hpp"
#include "op/OpBase.hpp"
#include "op/TargetBase.hpp"

#include <atomic>
#include <string>
#include <vector>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// Thread safe chaining hashtable
class Hashtable {
    public:
    /// A hashtable entry consists of a next pointer and the tuple itself
    struct Entry {
        Entry* next;
        uint64_t tuple[];
    };

#if defined(__AVX512F__)
#define _AVX_JOINFILTER
#endif

#ifdef AVX_JOINFILTER
    using hash_type = uint32_t;
    static constexpr std::pair<hash_type, uint32_t> computeHashes(uint32_t key) {
        // the fibonacci hashing constant
        return {key * 0x85ebca6b, key * 0xc2b2ae35};
    }
#else
    using hash_type = uint64_t;
    static constexpr std::pair<hash_type, uint32_t> computeHashes(uint32_t key) {
        // the fibonacci hashing constant
        auto val = key * 11400714819323198485llu;
        return {val, static_cast<uint32_t>(val)};
    }
#endif
    static constexpr size_t hashSize = sizeof(hash_type);
    static constexpr size_t hashBits = hashSize * 8;

    public:
    /// Shift
    size_t shift = 0;
    /// The hashtable
    uint64_t* ht = nullptr;
    /// The bloom filter
    uint16_t* bloom = nullptr;
    /// Get the number of tuples
    size_t numTuples = 0;
    /// The number of keys
    size_t numKeys = 0;
    /// Are we certainly duplicate free?
    bool isCertainlyDuplicateFree = true;

    friend struct HashtableBuild;
    friend struct HashtableProbe;

    /// The pretty name for debugging
    std::string pretty;

    /// The first index is next pointer, the second index is multiplicity
    static constexpr size_t keyOffset = config::handleMultiplicity ? 1 : 0;
    /// Get the hash table size
    [[nodiscard]] size_t htSize() const { return 1ull << (hashBits - shift); }
    /// Get the number of tuples
    [[nodiscard]] size_t getNumTuples() const { return numTuples; }
    /// Get the number of keys
    [[nodiscard]] size_t getNumKeysEstimate() const { return numKeys; }
    /// Is the hash table empty?
    [[nodiscard]] bool isEmpty() const { return numTuples == 0; }
    /// Is the hash table duplicate free?
    /// TODO: computing this will help reduce pipeline lengths
    [[nodiscard]] bool isDuplicateFree() const {
        /// TODO: we don't know, so return false to be safe
        return isCertainlyDuplicateFree;
    }
    /// Join filter
    [[gnu::always_inline]] inline bool joinFilter(uint64_t key) const {
        auto [h, b] = computeHashes(key);
        auto entry = bloom[h >> shift];
        return JoinFilter::checkEntry(b, entry);
    }
    /// Precise join filter
    [[gnu::always_inline]] inline bool joinFilterPrecise(uint64_t key) const {
        auto [h, b] = computeHashes(key);
        auto entry = bloom[h >> shift];
        if (!JoinFilter::checkEntry(b, entry))
            return false;
        auto* current = reinterpret_cast<Entry*>(ht[h >> shift]);
        // We assert current as join filters guarantee the slot won't be empty
        // Note that finish consume also relies on this assumption to not check the pointer when removing duplicates
        assert(current);
        do {
            if (current->tuple[Hashtable::keyOffset] == key)
                return true;
            current = current->next;
        } while (current);
        return false;
    }
    /// Allocate the hashtable to a reasonable size. Approx 12.5% larger than numElements. At least 16.
    void allocateHashtable(size_t numElements);
    /// Eq restrictions
    struct EqRestriction {
        unsigned offset;
        uint32_t value;
    };
    /// Filter the table with eq restrictions
    void filterEq(const EqRestriction& restrictions);

    /// Iterate over all keys found in the hash table
    template <typename CallbackT>
    void iterateAll(CallbackT&& callback) {
        for (size_t i = 0; i < htSize(); i++) {
            auto entry = ht[i];
            for (auto* current = reinterpret_cast<Entry*>(entry); current; current = current->next)
                callback(current->tuple[Hashtable::keyOffset]);
        }
    }
};
//---------------------------------------------------------------------------
/// Build sub operator
struct HashtableBuild : public TargetImpl<HashtableBuild> {
    /// The maximum number of partitions shift
    static constexpr size_t maxPartitionsShift = 7;
    /// The maximum number of partitions
    static constexpr size_t maxPartitions = 1ull << maxPartitionsShift;
    /// The reference to a current chunk within a partition
    struct ChunkRef {
        uint64_t* cur = nullptr;
        const uint64_t* end = nullptr;
    };
    /// An chunk
    struct Chunk;
    /// A block
    struct Block;

    /// Local state
    struct LocalState {
        /// The number of collected tuplese
        size_t numTuples = 0;
        /// The shift for finding partition
        size_t partitionShift;
        /// The partitions
        std::array<ChunkRef, maxPartitions> partitions;
        /// The linked list of chunks per partition
        std::array<Chunk*, maxPartitions> chunks;
        /// The linked list of blocks
        Block* blocks = nullptr;
        /// The first allocated block
        Block* tail = nullptr;
        /// The number of attributes
        size_t attrCount = 0;
        /// The next local state
        LocalState* next = nullptr;

        /// Allocate a chunk
        void allocateChunk(uint32_t partition, size_t attrCount);

        explicit LocalState(HashtableBuild& build);
    };

    /// The hashtable
    Hashtable& ht;
    /// The mask for partitions
    size_t partitionShift;
    /// References to the local states
    std::atomic<LocalState*> localStateRefs = nullptr;

    /// Should we build a cross product table or a normal table?
    bool isCrossProduct = false;
    /// Add tuple to tuple materialization
    template <typename... AttrT>
    void operator()(LocalState& ls, uint64_t multiplicity, uint64_t key, AttrT... attrs) {
        constexpr size_t attrCount = sizeof...(attrs) + 2 + config::handleMultiplicity;

        ls.numTuples++;

        auto hash = Hashtable::computeHashes(key).first;
        auto partition = hash >> partitionShift;
        assert(partition < maxPartitions);
        auto& part = ls.partitions[partition];
        if (part.cur == part.end) [[unlikely]] {
            // We need to allocate a new chunk
            ls.allocateChunk(partition, attrCount);
        }

        assert(part.cur + attrCount <= part.end);

        struct Entry {
            Entry* next;
            uint64_t tuple[sizeof...(attrs) + 1 + config::handleMultiplicity];
        };
        static_assert(sizeof(Entry) == sizeof(uint64_t) * attrCount);
        if constexpr (config::handleMultiplicity) {
            new (part.cur) Entry{nullptr, {multiplicity, key, attrs...}};
        } else {
            new (part.cur) Entry{nullptr, {key, attrs...}};
        }
        part.cur += attrCount;
    }
    /// Finish tuples
    void finishConsume();
    /// Constructor
    explicit HashtableBuild(Hashtable& ht, size_t cardEstimate);
    std::string getPretty() const override;
};
//---------------------------------------------------------------------------
/// Probe sub operator
struct HashtableProbe : OpBase {
    const Hashtable* ht;

    struct LocalState {
        explicit constexpr LocalState(HashtableProbe&) noexcept {}
    };

    explicit HashtableProbe(const Hashtable* ht) : ht(ht) {}

    void prepare(LocalState& ls, uint64_t key) {
        auto h = Hashtable::computeHashes(key).first;
        uint64_t entry = ht->ht[h >> ht->shift];
        auto* current = reinterpret_cast<Hashtable::Entry*>(entry);
        __builtin_prefetch(current, 0, 0); // read+nta
    };

    template <typename KeyT, typename ConsumerType, typename = std::enable_if_t<Consumer<ConsumerType>>>
    [[gnu::always_inline]] void operator()(LocalState& ls, KeyT key, ConsumerType&& consumer) {
        auto h = Hashtable::computeHashes(key).first;
        uint64_t entry = ht->ht[h >> ht->shift];
        const auto* current = reinterpret_cast<Hashtable::Entry*>(entry);
        // Since we nest filters, we do not guarantee we will find an element in the hash table
        if (!current) [[unlikely]]
            return;
        do {
            if (current->tuple[Hashtable::keyOffset] == key)
                consumer([current](unsigned idx) { return current->tuple[idx]; });
            current = current->next;
        } while (current);
    }
    std::string getPretty() const override;
};
//---------------------------------------------------------------------------
static_assert(TargetOperator<HashtableBuild, 1>);
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
