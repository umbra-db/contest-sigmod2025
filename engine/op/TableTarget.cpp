#include "op/TableTarget.hpp"
#include "infra/PageMemory.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/Scheduler.hpp"
#include "query/DataSource.hpp"
#include "storage/StringPtr.hpp"
#include <cstdint>
#include <numeric>
#include <attribute.h>
#include <plan.h>
#ifdef __x86_64__
#include <immintrin.h>
#endif
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
namespace impl {
//---------------------------------------------------------------------------
struct Writer {
    static constexpr uint64_t nullValue = RuntimeValue::nullValue;

    /// The pages
    Vector<DataSource::Page*> pages;

    /// Allocate a page
    DataSource::Page* allocatePage();

    /// Write a value multiple times
    virtual void stepMany(uint64_t value, size_t count) = 0;
    /// Write multiple values
    virtual void stepBatch(uint64_t* multiplicities, uint64_t* values, size_t count, bool hasNulls) = 0;
    /// Flush collected
    virtual void flush() = 0;

    /// Destructor
    virtual ~Writer() noexcept { assert(pages.empty()); }
};
//---------------------------------------------------------------------------
/// Allocate a page
DataSource::Page* Writer::allocatePage() {
    DataSource::Page* res;
    res = pages.emplace_back(static_cast<DataSource::Page*>(pagememory::allocate()));
    res->numRows = 0;
    res->numNotNull = 0;
    return res;
}
//---------------------------------------------------------------------------
class NullStore {
    size_t size_ = 0;
    std::array<uint8_t, 8192> nulls{}; // common case: i32, no nulls

    public:
    void push_back(bool isValid) {
        assert(size_ / 8 < nulls.size());
        if (size_ % 8 == 0)
            assert(nulls[size_ / 8] == 0);
        nulls[size_ / 8] |= static_cast<uint8_t>(isValid) << (size_ % 8);
        size_++;
    }
    void push_many_false(size_t count) {
        // For null values (i.e. false) nothing needs to be OR-ed in,
        // we only need to update size_ and ensure the vector is large enough.
        size_ += count;
    }
    void push_many_true(size_t count) {
        // Append 'count' valid bits (set to 1).
        size_t countRemaining = count;

        size_t initial = std::min(8 - size_ % 8, countRemaining);
        nulls[size_ / 8] |= ((1 << initial) - 1) << (size_ % 8);
        countRemaining -= initial;
        size_ += initial;
        if (!countRemaining)
            return;

        assert(size_ % 8 == 0);
        size_t bytes = countRemaining / 8;
        memset(nulls.data() + size_ / 8, 0xff, bytes);
        size_ += bytes * 8;
        assert(size_ % 8 == 0);
        countRemaining -= bytes * 8;
        assert(countRemaining < 8);
        nulls[size_ / 8] |= ((1 << countRemaining) - 1);
        size_ += countRemaining;
    }
    void push_many(bool isValid, size_t count) {
        if (!isValid) {
            push_many_false(count);
        } else {
            push_many_true(count);
        }
    }
    const uint8_t* data() const {
        return nulls.data();
    }
    size_t size() const {
        return size_;
    }
    size_t byteSize() const {
        return (size_ + 7) / 8;
    }
    bool empty() const {
        return size_ == 0;
    }
    void clear() {
        memset(nulls.data(), 0, (size_ + 7) / 8);
        size_ = 0;
    }
};
/// A writer for a column
template <typename T, bool isString = false>
struct WriterT : Writer {
    /// The available space in bits
    int64_t available = CHAR_BIT * (DataSource::PAGE_SIZE - 2 * sizeof(uint16_t) - (sizeof(T) == 8 ? 4 : 0));
    T* next;
    NullStore nulls;

    WriterT() : Writer(), next(allocatePage()->template getData<T>()) {}

    /// Flush current page
    void flushPage() {
        pages.back()->numRows = nulls.size();
        pages.back()->numNotNull = next - pages.back()->template getData<T>();
        memcpy(pages.back()->getNulls(), nulls.data(), nulls.byteSize());
        nulls.clear();
        next = allocatePage()->template getData<T>();
        available = CHAR_BIT * (DataSource::PAGE_SIZE - 2 * sizeof(uint16_t) - (sizeof(T) == 8 ? 4 : 0));
    }
    /// Flush collected
    void flush() override {
        if (nulls.empty())
            return;
        // no reset and new allocation required as this is the end...
        pages.back()->numRows = nulls.size();
        pages.back()->numNotNull = next - pages.back()->template getData<T>();
        memcpy(pages.back()->getNulls(), nulls.data(), nulls.byteSize());
        nulls.clear();
    }

    /// Write a value
    void step(uint64_t val) {
        available -= 1; // everything requires at least one bit
        available -= (val == nullValue ? 0 : sizeof(T) * CHAR_BIT);
        if (available < 0) [[unlikely]] {
            flushPage();
            available -= 1 + (val == nullValue ? 0 : sizeof(T) * CHAR_BIT);
        }
        // branchless should be beneficial as most of the times the values is non-null
        *next = (T) val;
        next += val != nullValue;
        nulls.push_back(val != nullValue);
    }

    void step_not_null(uint64_t val) {
        available -= 1; // everything requires at least one bit
        available -= sizeof(T) * CHAR_BIT;
        if (available < 0) [[unlikely]] {
            flushPage();
            available -= 1 + sizeof(T) * CHAR_BIT;
        }
        // branchless should be beneficial as most of the times the values is non-null
        *next = (T) val;
        next += 1;
        nulls.push_back(true);
    }

    /// Write a value multiple times
    void stepMany(uint64_t val, size_t count) override {
        for (size_t i = 0; i < count; i++)
            step(val);
    }
    /// Write a value multiple times
    void stepBatch(uint64_t* multiplicities, uint64_t* values, size_t count, bool hasNulls) override {
        if constexpr (config::handleMultiplicity) {
            for (size_t i = 0; i < count; i++)
                stepMany(values[i], multiplicities[i]);
        } else {
            for (size_t i = 0; i < count; i++)
                step(values[i]);
        }
    }
};
template <>
struct WriterT<uint16_t, true> : Writer {
    using T = uint16_t;

    char* strings;
    size_t stringsSize = 0;
    /// The available space in bits
    int64_t available = CHAR_BIT * (DataSource::PAGE_SIZE - 2 * sizeof(uint16_t));
    static constexpr int64_t maxSmallString = DataSource::PAGE_SIZE - 7;
    T* next;
    NullStore nulls;

    WriterT() : Writer(), next(allocatePage()->getData<T>()) {
        strings = static_cast<char*>(querymemory::allocate(PAGE_SIZE));
    }
    /// Flush current page
    void flushPage() {
        pages.back()->numRows = nulls.size();
        pages.back()->numNotNull = next - pages.back()->template getData<T>();
        memcpy(pages.back()->getNulls(), nulls.data(), nulls.byteSize());
        memcpy(pages.back()->getStrings(), strings, stringsSize);
        nulls.clear();
        stringsSize = 0;
        next = allocatePage()->template getData<T>();
        available = CHAR_BIT * (DataSource::PAGE_SIZE - 2 * sizeof(uint16_t) - (sizeof(T) == 8 ? 4 : 0));
    }
    /// Flush collected
    void flush() override {
        if (nulls.empty()) [[unlikely]]
            return;
        pages.back()->numRows = nulls.size();
        pages.back()->numNotNull = next - pages.back()->template getData<T>();
        memcpy(pages.back()->getNulls(), nulls.data(), nulls.byteSize());
        memcpy(pages.back()->getStrings(), strings, stringsSize);
    }
    /// Write a value
    void step(uint64_t val) {
        if (val == nullValue) {
            available -= 1;
            if (available < 0) [[unlikely]] {
                flushPage();
                available -= 1;
            }
            nulls.push_back(false);
            return;
        }
        StringPtr str{val};
        if (!str.is_long()) {
            available -= 1;
            available -= 16; // the size
            available -= CHAR_BIT * str.length();
            if (available < 0) [[unlikely]] {
                flushPage();
                available -= 1 + 16 + (CHAR_BIT * str.length());
            }
            memcpy(strings + stringsSize, str.str(), str.length());
            stringsSize += str.length();
            *next = stringsSize;
            next += 1;
            nulls.push_back(true);
        } else {
            flushPage();
            for (auto* page : str.pages()) {
                auto* ptr = pages.back();
                memcpy(ptr, page, DataSource::PAGE_SIZE);
                next = allocatePage()->getData<T>();
            }
        }
    }

    void writeStringFast(StringPtr str) {
        assert(!str.is_long());
        auto string_len = str.length();
        char* dest = strings + stringsSize;
        if (string_len <= 6) {
            unalignedStore(dest, str.val() >> 16);
        } else {
            auto* ptr = str.strLong();
            if (string_len <= 8) {
                unalignedStore(dest, unalignedLoad<uint32_t>(ptr));
                unalignedStore(dest + string_len - 4, unalignedLoad<uint32_t>(ptr + string_len - 4));
            } else if (string_len <= 16) {
                unalignedStore(dest, unalignedLoad<uint64_t>(ptr));
                unalignedStore(dest + string_len - 8, unalignedLoad<uint64_t>(ptr + string_len - 8));
            } else [[unlikely]] {
                memcpy(dest, ptr, string_len);
            }
        }
        stringsSize += string_len;
    }
    /// Write a value multiple times
    void stepMany(uint64_t val, size_t count) override {
        if (val == nullValue) {
            // For null values, each uses 1 bit.
            while (count > 0) {
                if (available < 1) {
                    flushPage();
                    continue;
                }
                // Determine how many nulls we can append on this page.
                size_t batch = std::min(count, static_cast<size_t>(available));
                available -= batch;
                nulls.push_many(false, batch);
                count -= batch;
            }
            return;
        }
        StringPtr str{val};
        if (str.is_long()) {
            // For long strings, we must flush for each.
            while (count--) {
                step(val);
            }
            return;
        }
        size_t string_len = str.length();

        // Each non-null entry costs: 1 (null flag) + 16 (size) + CHAR_BIT * string_len bits.
        int64_t delta = 1 + 16 + CHAR_BIT * string_len;

        // Did we flush a clean page with only our strings?
        bool flushed = false;
        if (available < delta) [[unlikely]] {
            flushPage();
            flushed = true;
        }

        size_t prevBatch = ~0ull;
        while (count > 0) {
            // How many iterations can we process on this page?
            size_t batch = static_cast<size_t>(available / delta);
            if (batch > count)
                batch = count;
            if (batch == prevBatch) {
                assert(pages.back()->numRows == 0);
                assert(nulls.empty());
                assert(stringsSize == 0);
                assert(pages[pages.size() - 2]->numRows == batch);
                memcpy(pages.back(), pages[pages.size() - 2], DataSource::PAGE_SIZE);
                count -= batch;
                next = allocatePage()->template getData<T>();
                continue;
            }

            // Exponential doubling copy:
            char* dest = strings + stringsSize;
            // First copy: copy one instance of the string.
            memcpy(dest, str.str(), string_len);
            size_t copied = 1;
            // Double the copied region until we've copied 'batch' instances.
            while (copied < batch) {
                size_t to_copy = std::min(copied, batch - copied);
                memcpy(dest + copied * string_len, dest, to_copy * string_len);
                copied += to_copy;
            }

            // Update the offsets for each copied string.
            // The offsets are cumulative; the first string ends at (baseOffset + string_len),
            // the next at (baseOffset + 2 * string_len), etc.
            uint16_t baseOffset = static_cast<uint16_t>(stringsSize);
            for (size_t i = 0; i < batch; i++) {
                next[i] = static_cast<uint16_t>(baseOffset + (i + 1) * string_len);
            }
            stringsSize += batch * string_len;
            next += batch;
            available -= batch * delta;
            nulls.push_many(true, batch);
            count -= batch;
            if (flushed)
                prevBatch = batch;

            if (count > 0) {
                assert(available < delta);
                flushPage();
                flushed = true;
            }
        }
    }
    /// Write a value multiple times
    void stepBatch(uint64_t* multiplicities, uint64_t* values, size_t size, bool) override {
        if constexpr (config::handleMultiplicity) {
            for (size_t i = 0; i < size; i++) {
                auto val = values[i];
                auto mult = multiplicities[i];
                if (multiplicities[i] == 1 && !(val & (1ull << 15))) [[likely]] {
                    assert(val != RuntimeValue::nullValue);
                    StringPtr str{values[i]};
                    assert(!str.is_long());
                    auto string_len = str.length();
                    int64_t delta = 1 + 16 + CHAR_BIT * string_len;
                    if (available < delta) [[unlikely]]
                        flushPage();

                    writeStringFast(str);

                    *next = stringsSize;
                    available -= delta;
                    next += 1;
                    nulls.push_back(true);
                } else {
                    stepMany(values[i], multiplicities[i]);
                }
            }
        } else {
            for (size_t i = 0; i < size; i++)
                step(values[i]);
        }
    }
};
}
//---------------------------------------------------------------------------
template <typename Allocator>
static void copyPage(std::vector<Page*, Allocator>& pages, engine::span<DataSource::Page*> source, size_t count, bool moveInitial) {
    if (count == 0)
        return;
    pages.reserve(source.size() * count);
    if (moveInitial) {
        for (auto* page : source)
            pages.push_back(reinterpret_cast<Page*>(page));
        count--;
    }

    for (size_t i = 0; i < count; i++) {
        for (auto* page : source) {
            auto* p = pages.emplace_back(new Page);
            memcpy(p, page, PAGE_SIZE);
        }
    }
}
//---------------------------------------------------------------------------
ColumnarTable TableTarget::prepareAndExtract(const SmallVec<std::variant<unsigned, RuntimeValue>>& columns) {
    ColumnarTable result;
    {
        pagememory::AllocationStealer stealer;
        result.columns.reserve(columns.size());
    }
    result.num_rows = std::accumulate(localStates.begin(), localStates.end(), 0ul,
                                      [](const size_t acc, const auto& state) { return acc + state->numRows; });
    SmallVec<unsigned> writerUsed(localStates.front()->writers.size(), ~0u);
    for (size_t colIdx = 0; colIdx < columns.size(); colIdx++) {
        auto& column = columns[colIdx];
        if (std::holds_alternative<unsigned>(column)) {
            const auto idx = get<unsigned>(column);
            auto& col = result.columns.emplace_back(types[idx]);
            size_t totCount = 0;
            for (auto& state : localStates)
                totCount += state->writers[idx]->pages.size();
            // Unfortunately, it seems std::malloc can be arbitrarily slow
            // So we steal allocations here
            {
                pagememory::AllocationStealer stealer;
                col.pages.reserve(totCount);
            }

            // we can put the pages into multiple output columns WITHOUT a copy
            // the only potential problem might be a "double-free" on the page, but as
            // we catch those cases and make them a nop anyways, there is no problem with that
            // (we only also have to convince the address sanitizer...)
            bool used = writerUsed[idx] != ~0u;
            if (!used) {
                for (auto& state : localStates) {
                    for (auto* page : state->writers[idx]->pages)
                        col.pages.push_back(reinterpret_cast<Page*>(page));
                }
                writerUsed[idx] = colIdx;
            } else {
                auto& srcCol = result.columns[writerUsed[idx]].pages;
                copyPage(col.pages, engine::span{reinterpret_cast<DataSource::Page**>(srcCol.data()), srcCol.size()}, 1, false);
            }
        } else {
            auto value = get<RuntimeValue>(column);
            auto& col = result.columns.emplace_back(value.type);
            auto vv = value.value;

            if (result.num_rows == 0) continue;

            auto writer = makeWriter(value.type);

            writer->stepMany(vv, result.num_rows);
            writer->flush();
            {
                pagememory::AllocationStealer stealer;
                col.pages.reserve(writer->pages.size());
            }
            for (auto* page : writer->pages)
                col.pages.push_back(reinterpret_cast<Page*>(page));
            writer->pages.clear();
        }
    }

    // Clear the writers
    for (size_t idx = 0; idx < writerUsed.size(); idx++) {
        if (writerUsed[idx] == ~0u) {
            for (auto& state : localStates) {
                for (auto* page : state->writers[idx]->pages)
                    delete page;

                state->writers[idx]->pages.clear();
            }
        } else {
            for (auto& state : localStates) {
                state->writers[idx]->pages.clear();
            }
        }
    }

    return result;
}
//---------------------------------------------------------------------------
ColumnarTable TableTarget::extract() {
    ColumnarTable result;
    result.num_rows = std::accumulate(localStates.begin(), localStates.end(), 0ul,
                                      [](const size_t acc, const auto* state) { return acc + state->numRows; });
    assert(localStates.front()->writers.size() == types.size());
    {
        pagememory::AllocationStealer stealer;
        result.columns.reserve(localStates.front()->writers.size());
    }

    for (size_t i = 0; i < localStates.front()->writers.size(); i++) {
        auto& col = result.columns.emplace_back(types[i]);
        size_t pageCount = 0;
        for (auto* state : localStates)
            pageCount += state->writers[i]->pages.size();

        {
            pagememory::AllocationStealer stealer;
            col.pages.reserve(pageCount);
        }

        for (auto* state : localStates) {
            for (auto* page : state->writers[i]->pages)
                col.pages.push_back(reinterpret_cast<Page*>(page));
            state->writers[i]->pages.clear();
        }
    }

    return result;
}
//---------------------------------------------------------------------------
/// Make a writer given type
UniquePtr<impl::Writer> TableTarget::makeWriter(DataType type) {
    switch (type) {
        case DataType::INT32: return makeUnique<impl::WriterT<uint32_t>>();
        case DataType::INT64: return makeUnique<impl::WriterT<uint64_t>>();
        case DataType::FP64: return makeUnique<impl::WriterT<uint64_t>>();
        case DataType::VARCHAR: return makeUnique<impl::WriterT<uint16_t, true>>();
    }
    __builtin_unreachable();
}
//---------------------------------------------------------------------------
TableTarget::LocalState::LocalState(TableTarget& target) {
    for (auto type : target.types)
        writers.emplace_back(makeWriter(type));
    // The first column is multiplicity
    buffer = static_cast<uint64_t*>(querymemory::allocate(sizeof(uint64_t) * bufferCount * (config::handleMultiplicity + writers.size())));
    curBuffer = buffer;
    endBuffer = buffer + bufferCount;
    next = target.localStateRefs.exchange(this);
}
//---------------------------------------------------------------------------
TableTarget::LocalState::~LocalState() noexcept = default;
TableTarget::LocalState::LocalState(LocalState&&) noexcept = default;
TableTarget::LocalState& TableTarget::LocalState::operator=(LocalState&&) noexcept = default;
//---------------------------------------------------------------------------
/// Constructor
TableTarget::TableTarget(SmallVec<DataType>&& types) : types(types) {
}
//---------------------------------------------------------------------------
/// Flush collected
void TableTarget::finishConsume() {
    localStates.reserve(Scheduler::concurrency());
    for (auto* state = localStateRefs.load(); state; state = state->next)
        localStates.push_back(state);
}
//---------------------------------------------------------------------------
void TableTarget::finalize(LocalState& state) const {
    state.flushBuffers();
    for (auto& writer : state.writers)
        writer->flush();
}
//---------------------------------------------------------------------------
void TableTarget::LocalState::flushBuffers() {
    auto numRowsBuffer = (curBuffer - buffer);
    if (!numRowsBuffer)
        return;
    if constexpr (config::handleMultiplicity) {
        for (size_t i = 0; i < numRowsBuffer; i++) {
            assert(buffer[i] >= 1);
            numRows += buffer[i];
        }
    } else {
        numRows += numRowsBuffer;
    }
    for (size_t i = 0; i < writers.size(); i++) {
        auto* st = buffer + (i + config::handleMultiplicity) * bufferCount;
        writers[i]->stepBatch(buffer, st, numRowsBuffer, hasNulls & (1u << i));
    }
    curBuffer = buffer;
    hasNulls = 0;
}
//---------------------------------------------------------------------------
std::string TableTarget::getPretty() const {
    return "out";
}
//---------------------------------------------------------------------------
/// Destructor
TableTarget::~TableTarget() noexcept {
    for (auto* ls : localStates) {
        ls->~LocalState();
    }
}
//---------------------------------------------------------------------------
}
