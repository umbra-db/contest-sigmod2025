#include "op/Hashtable.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/Scheduler.hpp"
#include "infra/helper/BitOps.hpp"
#include "query/DataSource.hpp"
#include "query/RuntimeValue.hpp"
#include <numeric>
#include <unordered_set>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
namespace {
// The maximum partition shift in a hashtable
constexpr size_t htPartitionShiftLimit = HashtableBuild::maxPartitionsShift;
}
//---------------------------------------------------------------------------
/// The size of a chunk
static constexpr size_t chunkSize = 8048;
/// The number of attrs in a chunk
static constexpr size_t chunkCount = chunkSize / sizeof(uint64_t) - 2;
/// The number of chunks in a block
static constexpr size_t blockChunks = HashtableBuild::maxPartitions - 1;
static_assert(blockChunks < (1ull << 16));
//---------------------------------------------------------------------------
/// An chunk
struct HashtableBuild::Chunk {
    /// The next chunk within partition
    Chunk* next;
    /// The end of the data
    size_t end;
    /// The data
    uint64_t data[chunkCount];
};
static_assert(sizeof(HashtableBuild::Chunk) == chunkSize);
/// A block
struct alignas(4096) HashtableBuild::Block {
    /// The next block within allocated blocks
    Block* next;
    /// The upcoming chunk to allocate
    size_t currentChunk;
    /// The chunks
    Chunk chunks[blockChunks];
};
//---------------------------------------------------------------------------
std::string HashtableBuild::getPretty() const {
    return "ht";
}
//---------------------------------------------------------------------------
std::string HashtableProbe::getPretty() const {
    return ht->pretty;
}
//---------------------------------------------------------------------------
HashtableBuild::LocalState::LocalState(HashtableBuild& build) : partitionShift(build.partitionShift) {
    auto numPartitions = 1ull << (Hashtable::hashBits - partitionShift);
    memset(partitions.data(), 0, sizeof(ChunkRef) * numPartitions);
    memset(chunks.data(), 0, sizeof(Chunk*) * numPartitions);
    next = build.localStateRefs.exchange(this);
}
//---------------------------------------------------------------------------
void HashtableBuild::LocalState::allocateChunk(uint32_t partition, size_t attrCountInp) {
    // Allocate a block if full
    if (!blocks || (blocks->currentChunk == blockChunks)) {
        attrCount = attrCountInp;
        static_assert(sizeof(Block) % DataSource::PAGE_SIZE == 0);
        auto* newBlock = static_cast<Block*>(querymemory::allocate(sizeof(Block)));
        newBlock->next = blocks;
        newBlock->currentChunk = 0;
        blocks = newBlock;
        if (!tail)
            tail = newBlock;
    }
    assert(attrCountInp == attrCount);

    auto& part = partitions[partition];
    // Update the end position of the previous chunk
    if (chunks[partition]) {
        assert(part.cur);
        assert(part.cur >= chunks[partition]->data);
        assert(part.cur <= chunks[partition]->data + chunkCount);
        chunks[partition]->end = part.cur - chunks[partition]->data;
    }

    // Allocate the new chunk
    auto* newChunk = &blocks->chunks[blocks->currentChunk++];
    newChunk->next = chunks[partition];
    newChunk->end = 0;
    chunks[partition] = newChunk;

    // Setup the partition
    part.cur = newChunk->data;
    part.end = newChunk->data + chunkCount - (chunkCount % attrCount);
}
//---------------------------------------------------------------------------
void Hashtable::allocateHashtable(size_t numElements) {
    auto sizeShift = std::max(engine::bit_width(numElements), 4);
    shift = hashBits - sizeShift;

    // Allocate the hashtable
    auto numEntries = htSize();
    auto allocBytes = numEntries * (sizeof(uint64_t) + sizeof(uint16_t));
    void* mem = querymemory::allocate(allocBytes);
    ht = static_cast<uint64_t*>(mem);
    bloom = reinterpret_cast<uint16_t*>(ht + numEntries);
}
//---------------------------------------------------------------------------
void Hashtable::filterEq(const EqRestriction& restriction) {
    auto morselSize = std::max<size_t>(htSize() / Scheduler::concurrency(), 100);
    uint64_t* foundSlot = nullptr;
    Scheduler::parallelFor(0, htSize(), morselSize, [&](size_t workerId, size_t start) {
        uint64_t removedTupleCount = 0;
        uint64_t removedSlotCount = 0;
        for (size_t i = start; i < std::min(start + morselSize, htSize()); i++) {
            if (!ht[i])
                continue;
            uint64_t* owner = &ht[i];
            while (true) {
                auto* tuple = reinterpret_cast<uint64_t*>(*owner);
                assert(tuple[restriction.offset + 1] != RuntimeValue::nullValue);
                if (tuple[restriction.offset + 1] != restriction.value) {
                    // Remove the tuple
                    *owner = tuple[0];
                    removedTupleCount++;
                }
                if (!*owner)
                    break;
                owner = reinterpret_cast<uint64_t*>(*owner);
                if (!*owner)
                    break;
            }
            if (!ht[i])
                removedSlotCount++;
        }
        // std::atomic_ref(numKeys).fetch_sub(removedSlotCount);
        __atomic_fetch_sub(&numKeys, removedSlotCount, __ATOMIC_SEQ_CST);
        // std::atomic_ref(numTuples).fetch_sub(removedTupleCount);
        __atomic_fetch_sub(&numTuples, removedTupleCount, __ATOMIC_SEQ_CST);
    });
}
//---------------------------------------------------------------------------
template <typename Callback>
static void iterateTuples(HashtableBuild* htb, size_t partition, Callback&& callback) {
    for (auto* ls = htb->localStateRefs.load(); ls; ls = ls->next) {
        auto attrCount = ls->attrCount;
        // Update the start position of the last chunk
        if (ls->chunks[partition]) {
            HashtableBuild::Chunk* chunk = ls->chunks[partition];
            assert(ls->partitions[partition].cur);
            assert(ls->partitions[partition].cur >= chunk->data);
            assert(ls->partitions[partition].cur <= chunk->data + chunkCount);
            chunk->end = ls->partitions[partition].cur - chunk->data;

            do {
                for (auto *tuple = chunk->data, *end = chunk->data + chunk->end; tuple < end; tuple += attrCount) {
                    callback(*reinterpret_cast<Hashtable::Entry*>(tuple));
                }
                chunk = chunk->next;
            } while (chunk);
        }
    }
};
//---------------------------------------------------------------------------
template <size_t AttributeCount>
[[gnu::always_inline]] static bool tupleEqual(Hashtable::Entry& t1, Hashtable::Entry& t2) {
    // Attribute count contains the header (the next pointer) as well
    return memcmp(t1.tuple + Hashtable::keyOffset, t2.tuple + Hashtable::keyOffset, (AttributeCount - 1 - Hashtable::keyOffset) * sizeof(uint64_t)) == 0;
}
//---------------------------------------------------------------------------
template <size_t AttributeCount>
[[gnu::always_inline]] static bool tupleRestEqual(Hashtable::Entry& t1, Hashtable::Entry& t2) {
    if constexpr (AttributeCount - 1 - Hashtable::keyOffset - 1 == 0)
        return true;
    // Attribute count contains the header (the next pointer) as well
    return memcmp(t1.tuple + Hashtable::keyOffset + 1, t2.tuple + Hashtable::keyOffset + 1, (AttributeCount - 1 - Hashtable::keyOffset - 1) * sizeof(uint64_t)) == 0;
}
//---------------------------------------------------------------------------
template <size_t AttributeCount>
static void finishConsumeCrossProductLogic(HashtableBuild* htb, size_t partition) {
    using namespace std;
    auto& ht = htb->ht;

    Hashtable::Entry* head = nullptr;
    Hashtable::Entry* tail = nullptr;
    iterateTuples(htb, partition, [&](Hashtable::Entry& tuple) {
        if (!head) {
            head = &tuple;
            tail = &tuple;
            if (config::handleMultiplicity && tuple.tuple[0] > 1)
                ht.isCertainlyDuplicateFree = false;
        } else {
            assert(!ht.isCertainlyDuplicateFree);
            if (config::handleMultiplicity && tupleEqual<AttributeCount>(*tail, tuple)) {
                tail->tuple[0] += tuple.tuple[0];
            } else {
                tail->next = &tuple;
                tail = &tuple;
            }
        }
    });
    if (head) {
        auto [h, b] = Hashtable::computeHashes(0);
        auto ind = h >> ht.shift;
        // tail[0] = std::atomic_ref(ht.ht[ind]).exchange(reinterpret_cast<uint64_t>(head));
        __atomic_exchange(&ht.ht[ind], reinterpret_cast<uint64_t*>(&head), reinterpret_cast<uint64_t*>(&tail->next), __ATOMIC_SEQ_CST);
    }
}
//---------------------------------------------------------------------------
template <size_t AttributeCount>
static void finishConsumeLogic(HashtableBuild* htb, size_t partition) {
    using namespace std;
    auto partitionCountShift = Hashtable::hashBits - htb->partitionShift;
    auto& ht = htb->ht;
    auto size = ht.htSize() >> partitionCountShift;

    {
        memset(ht.ht + partition * size, 0, size * sizeof(uint64_t));
        memset(ht.bloom + partition * size, 0, size * sizeof(uint16_t));
    }
    uint64_t localNumKeys = 0;
    uint64_t localRemovedTuples = 0;
    bool possibleDuplicate = false;
    const auto htShift = ht.shift;
    const auto htBuckets = ht.ht;
    iterateTuples(htb, partition, [&](Hashtable::Entry& tuple) {
        auto key = tuple.tuple[Hashtable::keyOffset];
        auto [h, b] = Hashtable::computeHashes(key);
        assert(h >> htb->partitionShift == partition);
        auto ind = h >> htShift;
        assert(ind >= partition * size);
        assert(ind < (partition + 1) * size);

        auto old = reinterpret_cast<Hashtable::Entry*>(htBuckets[ind]);
        auto mask = JoinFilter::getMask(b);
        auto& bloomEntry = ht.bloom[ind];
        if (config::handleMultiplicity)
            possibleDuplicate |= tuple.tuple[0] > 1;
        if (JoinFilter::checkMaskWithEntry(mask, bloomEntry)) {
            auto keyEq = old->tuple[Hashtable::keyOffset] == key;
            bool pd = keyEq || old->next;
            possibleDuplicate |= pd;
            localNumKeys += !pd;
            if (config::handleMultiplicity && keyEq && tupleRestEqual<AttributeCount>(*old, tuple)) {
                localRemovedTuples++;
                // Add multiplicities
                assert(tuple.tuple[0] >= 1);
                assert(old->tuple[0] >= 1);
                old->tuple[0] += tuple.tuple[0];
                return;
            }
        } else {
            localNumKeys++;
        }
        tuple.next = old;
        htBuckets[ind] = reinterpret_cast<uint64_t>(&tuple);
        bloomEntry |= mask;
    });

    // std::atomic_ref(ht.numKeys).fetch_add(localNumKeys);
    __atomic_fetch_add(&ht.numKeys, localNumKeys, __ATOMIC_SEQ_CST);
    if constexpr (config::handleMultiplicity)
        __atomic_fetch_sub(&ht.numTuples, localRemovedTuples, __ATOMIC_SEQ_CST);
    if (possibleDuplicate)
        __atomic_store_n(&ht.isCertainlyDuplicateFree, false, __ATOMIC_SEQ_CST);
};
//---------------------------------------------------------------------------
std::array<void (*)(HashtableBuild*, size_t), 16> finishConsumeLogics = ([]<size_t... Is>(std::index_sequence<Is...>) {
    return std::array<void (*)(HashtableBuild*, size_t), 16>{&finishConsumeLogic<Is>...};
})(std::make_index_sequence<16>{});
std::array<void (*)(HashtableBuild*, size_t), 16> finishConsumeCrossProductLogics = ([]<size_t... Is>(std::index_sequence<Is...>) {
    return std::array<void (*)(HashtableBuild*, size_t), 16>{&finishConsumeCrossProductLogic<Is>...};
})(std::make_index_sequence<16>{});
//---------------------------------------------------------------------------
void HashtableBuild::finishConsume() {
    using namespace std;

    ht.numTuples = 0;
    size_t attrCount = 2;
    for (auto* current = localStateRefs.load(); current; current = current->next) {
        ht.numTuples += current->numTuples;
        if (current->numTuples)
            attrCount = current->attrCount;
    }

    auto partitionCountShift = Hashtable::hashBits - partitionShift;
    auto numPartitions = 1ull << partitionCountShift;

    if (isCrossProduct) {
        // We will later check if that tuple has a multiplicity greater than 1g
        ht.isCertainlyDuplicateFree = ht.numTuples == 1;
        ht.numKeys = 1;
        ht.allocateHashtable(1);
        memset(ht.ht, 0, ht.htSize() * sizeof(uint64_t));
        memset(ht.bloom, 0, ht.htSize() * sizeof(uint16_t));
        auto [h, b] = Hashtable::computeHashes(0);
        ht.bloom[h >> ht.shift] = 0xffff;
    } else {
        ht.allocateHashtable(std::max<size_t>(ht.numTuples, numPartitions));
    }

    auto* logic = finishConsumeLogics[attrCount];
    if (isCrossProduct) {
        logic = finishConsumeCrossProductLogics[attrCount];
    }

    if (ht.numTuples <= 256 || !localStateRefs.load()->next) {
        for (size_t partition = 0; partition < numPartitions; ++partition) {
            logic(this, partition);
        }
    } else {
        Scheduler::parallelFor(0, numPartitions, [this, logic](size_t, size_t partition) { return logic(this, partition); });
    }

    // Compute duplicate freeness for small tables
    if (ht.numTuples <= 32) {
        std::unordered_set<uint32_t> keys;
        keys.reserve(ht.numTuples);
        bool hasMult = false;
        ht.iterateAll([&](uint64_t& key) {
            if constexpr (config::handleMultiplicity) {
                assert((&key)[-1] >= 1);
                if ((&key)[-1] != 1)
                    hasMult = true;
            }
            keys.insert(key);
        });
        [[maybe_unused]] bool prev = ht.isCertainlyDuplicateFree;
        ht.isCertainlyDuplicateFree = !hasMult && keys.size() == ht.numTuples;
        assert(!prev || ht.isCertainlyDuplicateFree);
    }

    if constexpr (!std::is_trivially_destructible_v<LocalState>) {
        for (auto* current = localStateRefs.load(); current; current = current->next)
            current->~LocalState();
    }
}
//---------------------------------------------------------------------------
HashtableBuild::HashtableBuild(Hashtable& ht, size_t cardEstimate) : ht(ht) {
    auto upper = std::min<size_t>(maxPartitionsShift, htPartitionShiftLimit);
    upper = std::min<size_t>(upper, Scheduler::concurrency() * 2);
    auto partitionCountShift = std::min<size_t>(std::max(engine::bit_width(cardEstimate / 1024), 2), upper);
    partitionShift = Hashtable::hashBits - partitionCountShift;
}
//---------------------------------------------------------------------------
}
