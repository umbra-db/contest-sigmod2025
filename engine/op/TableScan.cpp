#include "op/TableScan.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/Random.hpp"
#include "infra/Scheduler.hpp"
#include "infra/SmallVec.hpp"
#include "infra/Util.hpp"
#include "infra/helper/BitOps.hpp"
#include "infra/helper/Misc.hpp"
#include "storage/CopyLogic.hpp"
#include "storage/RestrictionLogic.hpp"
#include "storage/StringPtr.hpp"
#include <algorithm>
#include <unordered_map>
#if defined(__x86_64__) && defined(__BMI2__)
#include <immintrin.h>
#endif
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
static uint64_t pdep(uint64_t src, uint64_t mask) {
#if defined(__x86_64__) && defined(__BMI2__)
    return _pdep_u64(src, mask);
#elif defined(__PPC64__) && defined(__MMA__)
    return __builtin_pdepd(src, mask);
#else
    if (mask == ~0ull)
        return src;
    uint64_t result = 0;
    size_t shift = 0;
    while (mask && src) {
        auto ones = engine::countr_one(mask);
        result |= (src & ((1ull << ones) - 1)) << shift;
        src >>= ones;
        mask >>= ones;
        shift += ones;

        if (!mask)
            break;
        auto zeros = engine::countr_zero(mask);
        mask >>= zeros;
        shift += zeros;
    }

    return result;
#endif
}
//---------------------------------------------------------------------------
static uint64_t pext(uint64_t src, uint64_t mask) {
#if defined(__x86_64__) && defined(__BMI2__)
    return _pext_u64(src, mask);
#elif defined(__PPC64__) && defined(__MMA__)
    return __builtin_pextd(src, mask);
#else
    if (mask == ~0ull)
        return src;
    if (src == mask)
        return (1ull << engine::popcount(mask)) - 1;
    /// Assert that all bits of src are contained within mask
    /// Otherwise the loop condition will not work
    assert((src & ~mask) == 0);

    uint64_t result = 0;
    size_t shift = 0;
    while (src) {
        auto ones = engine::countr_one(mask);
        result |= (src & ((1ull << ones) - 1)) << shift;
        src >>= ones;
        mask >>= ones;
        shift += ones;

        if (!src)
            break;
        auto zeros = engine::countr_zero(mask);
        mask >>= zeros;
        src >>= zeros;
    }

    return result;
#endif
}
//---------------------------------------------------------------------------
struct TableScan::Reader {
    /// The start page
    DataSource::Page* const* startPage = nullptr;
    /// The current page
    DataSource::Page* const* curPage = nullptr;
    /// The end page
    DataSource::Page* const* endPage = nullptr;
    /// The search array
    const uint32_t* searchArray = nullptr;
    /// RowId of the current row in the whole column
    size_t rowId = 0;
    /// The current index within the page
    size_t tupleIndex = 0;
    /// The non-null index within the page
    size_t nonNullIndex = 0;
    /// Selectivity
    double selectivity = 1.0;

    virtual ~Reader() noexcept = default;

    virtual DataType getDataType() const noexcept = 0;

    Reader() = default;
    /// Constructor
    Reader(DataSource::Page* const* curPage, DataSource::Page* const* endPage, const uint32_t* searchArray);

    /// Are we at the end?
    bool done() const noexcept { return curPage == endPage; }

    /// Find first element that is not greater than requestedId
    /// Assumption: requestedId >= *st && requestedId < *(en - 1)
    static size_t exponentialSearch(const uint32_t* st, const uint32_t* en, size_t requestedId) {
        if (requestedId == 0) {
            assert(*st == 0);
            return 0;
        }
        assert(requestedId >= *st);
        assert(requestedId < *(en - 1));
        size_t n = en - st;
        size_t jump = 1;
        while (jump < n && st[jump] <= requestedId)
            jump *= 2;
        // assert(st[jump / 2] < requestedId);
        auto pos = std::upper_bound(st + jump / 2, st + std::min(jump, n), requestedId) - st;
        assert(pos <= n);
        assert(pos > 0);
        pos--;
        assert(st[pos] <= requestedId);
        assert(st[pos + 1] > requestedId);
        return pos;
    }

    // Skips to the specified rowId
    void skipTo(size_t requestedId) {
        if (requestedId == rowId)
            return;

        if (requestedId < rowId)
            curPage = startPage;

        // Should we skip the current page
        tupleIndex = 0;
        nonNullIndex = 0;
        auto pos = exponentialSearch(searchArray + (curPage - startPage), searchArray + (endPage + 1 - startPage), requestedId) + (curPage - startPage);
        rowId = searchArray[pos];
        curPage = startPage + pos;

        // Find the position within the page
        assert(rowId <= requestedId);
        assert(searchArray[pos + 1] - searchArray[pos] == (*curPage)->getContainedRows());
        assert(rowId + (*curPage)->getContainedRows() > requestedId);
        tupleIndex = requestedId - rowId;
        if ((*curPage)->hasNoNulls()) {
            nonNullIndex = tupleIndex;
        } else {
            // Find the number of non-null bits until tupleIndex
            size_t remaining = tupleIndex;
            uint8_t* bits = (*curPage)->getNulls();
            nonNullIndex = 0;
            auto process = [&]<typename U>(engine::type_identity<U>) {
                auto b = unalignedLoad<U>(bits);
                nonNullIndex += engine::popcount(b);
                remaining -= sizeof(U) * 8;
                bits += sizeof(U);
            };
            while (remaining >= 64)
                process(engine::type_identity<uint64_t>{});
            if (remaining >= 32)
                process(engine::type_identity<uint32_t>{});
            if (remaining >= 16)
                process(engine::type_identity<uint16_t>{});
            if (remaining >= 8)
                process(engine::type_identity<uint8_t>{});
            assert(nonNullIndex <= tupleIndex);
            nonNullIndex += engine::popcount(unalignedLoad<uint8_t>(bits) & ((1ull << remaining) - 1));
            assert(nonNullIndex <= tupleIndex);
        }
        rowId = requestedId;
    }

    virtual void step64(uint64_t* target, uint64_t matches, size_t numTuples) = 0;

    static uint64_t load64(const uint8_t* bits, const uint8_t* end) {
        if (bits + 8 <= end) [[likely]]
            return unalignedLoad<uint64_t>(bits);
        auto val = unalignedLoad<uint64_t>(end - 8);
        assert(end - bits <= 7);
        // If end == bits, we do not care what the bits are
        return val >> ((64 - (end - bits) * 8) & 63);
    }

    static uint64_t load64(const void* bits, const void* end) {
        return load64(static_cast<const uint8_t*>(bits), static_cast<const uint8_t*>(end));
    }

    uint64_t getNextNotNulls() const {
        if ((*curPage)->hasNoNulls())
            return ~0ull;
        auto* base = (*curPage)->getNulls() + tupleIndex / 8;
        return (load64(base + 1, (*curPage) + 1) << (8 - tupleIndex % 8)) | (*base >> (tupleIndex % 8));
    }

    void skipMany(size_t numTuples) {
        if (numTuples == 0)
            return;
        rowId += numTuples;
        if ((*curPage)->isLongStringStart()) [[unlikely]] {
            assert(numTuples <= 1);
            curPage++;
            for (; (*curPage)->isLongStringContinuation(); curPage++);
            tupleIndex = 0;
            nonNullIndex = 0;
            return;
        }
        assert(numTuples <= (*curPage)->numRows - tupleIndex);
        if ((*curPage)->hasNoNulls()) {
            nonNullIndex += numTuples;
            tupleIndex += numTuples;
        } else {
            while (numTuples >= 64) {
                auto notNulls = getNextNotNulls();
                nonNullIndex += engine::popcount(notNulls);
                tupleIndex += 64;
                numTuples -= 64;
            }
            if (numTuples) {
                auto notNulls = getNextNotNulls();
                nonNullIndex += engine::popcount(notNulls & (~0ull >> (64 - numTuples)));
                tupleIndex += numTuples;
            }
        }
        if (tupleIndex == (*curPage)->numRows) {
            curPage++;
            tupleIndex = 0;
            nonNullIndex = 0;
        }
    }
};
//---------------------------------------------------------------------------
TableScan::Reader::Reader(DataSource::Page* const* curPage, DataSource::Page* const* endPage, const uint32_t* searchArray)
    : startPage(curPage), curPage(curPage), endPage(endPage), searchArray(searchArray) {
}
//---------------------------------------------------------------------------
/// Reader for type
template <typename T>
struct ReaderT : public TableScan::Reader {
    using Reader::Reader;
    static constexpr bool isString = std::is_same_v<T, uint16_t>;
    std::array<uint8_t, 71> srcBuffer, dstBuffer;

    ReaderT() = default;

    DataType getDataType() const noexcept final {
        if constexpr (isString) {
            return DataType::VARCHAR;
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            return DataType::INT64;
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            return DataType::INT32;
        }
    }

    /// Get the next elements
    void step64(uint64_t* target, uint64_t matches, size_t numTuples) final {
        if (numTuples == 0)
            return;
        assert(numTuples <= (*curPage)->numRows - tupleIndex);
        auto backup = std::make_tuple(rowId, curPage, tupleIndex, nonNullIndex);
        assert(!done());
        if (isString && (*curPage)->isLongStringStart()) [[unlikely]] {
            assert(matches <= 1);
            assert(numTuples <= 1);
            auto* pages = curPage;
            curPage++;
            for (; (*curPage)->isLongStringContinuation(); curPage++);
            auto str = StringPtr::fromLongString((DataSource::Page**) pages, curPage - pages);
            uint64_t value = str.val();
            tupleIndex = 0;
            nonNullIndex = 0;
            if (matches) {
                *target = value;
            }
            rowId++;
            return;
        }
        uint64_t notNulls = getNextNotNulls();
        uint64_t srcOffsets = pext(matches & notNulls, notNulls);
        uint64_t dstOffsets = pext(matches & notNulls, matches);

        auto* baseData = (*curPage)->template getData<T>();
        auto* data = baseData + nonNullIndex;
        if constexpr (std::is_same<T, uint32_t>::value) {
            CopyLogic::extractInt32(target, data, srcOffsets, dstOffsets, numTuples);
        } else if constexpr (std::is_same<T, uint64_t>::value) {
            CopyLogic::extractInt64(target, data, srcOffsets, dstOffsets, numTuples);
        } else if constexpr (isString) {
            auto stringHead = (*curPage)->getStrings();
            // Is the very first element read?
            bool readFirst = (nonNullIndex == 0) && (srcOffsets & 1);
            uint64_t dstPos = 0;
            if (readFirst) {
                // The very first element has the starting offset 0
                srcOffsets -= 1;
                dstPos = engine::countr_zero(dstOffsets);
                dstOffsets -= 1ull << dstPos;
            }
            CopyLogic::extractVarChar(target, data, srcOffsets, dstOffsets, numTuples, stringHead);
            if (readFirst) {
                target[dstPos] = StringPtr::fromString(stringHead, data[0]).val();
            }
        } else {
            assert(false && "Unsupported type");
        }

        rowId += numTuples;
        tupleIndex += numTuples;
        nonNullIndex += (*curPage)->hasNoNulls() ? numTuples : engine::popcount(notNulls & (~0ull >> (64 - numTuples)));
        if (tupleIndex == (*curPage)->numRows) {
            curPage++;
            tupleIndex = 0;
            nonNullIndex = 0;
        }
    }
};
//---------------------------------------------------------------------------
/// Reader for type
struct TableScan::RestrictedReader final : public ReaderT<uint32_t> {
    const RestrictionLogic* applyRestriction;

    /// Constructor
    RestrictedReader(DataSource::Page* const* curPage, DataSource::Page* const* endPage, const uint32_t* searchArray, TableScan::RestrictionInfo restrictionFunc)
        : ReaderT<uint32_t>(curPage, endPage, searchArray), applyRestriction(restrictionFunc.restriction == RestrictionLogic::notNullRestriction ? nullptr : restrictionFunc.restriction) {
        selectivity = restrictionFunc.selectivity;
    }

    /// Check the upcoming elements for whether they match the restriction
    uint64_t peek64(uint64_t existingMask, size_t numTuples) {
        assert(numTuples != 0);
        assert(numTuples <= (*curPage)->numRows - tupleIndex);

        if (!applyRestriction)
            return (*curPage)->hasNoNulls() ? existingMask : existingMask & getNextNotNulls();

        uint64_t mask = existingMask;
        uint64_t notNulls = getNextNotNulls();
        mask &= notNulls;
        if (!mask)
            return 0;
        uint64_t srcOffsets = pext(mask, notNulls);

        auto* values = (*curPage)->template getData<uint32_t>() + nonNullIndex;

        uint64_t newMask = applyRestriction->run(values, srcOffsets);

        // Expand the newMask to notNulls
        uint64_t matches = pdep(newMask, notNulls);

        mask &= matches;

        return mask;
    }

    /// Check the upcoming elements for whether they match the restriction
    /// Skip as far as possible
    std::pair<uint64_t, size_t> peekFirst(size_t numTuples) {
        assert(!done());
        assert(numTuples != 0);
        assert(numTuples <= (*curPage)->numRows - tupleIndex);

        if ((*curPage)->hasNoNulls()) {
            // If we do not have any nulls and we have a not null restriction, we can skip nothing
            if (!applyRestriction)
                return {~0ull >> (64 - std::min<size_t>(numTuples, 64)), 0};
            assert(tupleIndex < (*curPage)->numRows);
            auto* values = (*curPage)->template getData<uint32_t>() + nonNullIndex;
            auto [mask, skipped] = applyRestriction->runAndSkip(values, numTuples);
            assert(skipped <= numTuples);
            tupleIndex += skipped;
            nonNullIndex += skipped;
            rowId += skipped;
            if (mask)
                return {mask, skipped};
        } else {
            size_t skipped = 0;
            while (skipped < numTuples) {
                assert(tupleIndex < (*curPage)->numRows);
                auto step = std::min<size_t>(numTuples - skipped, 64);
                uint64_t notNulls = getNextNotNulls();
                uint64_t mask = notNulls & (~0ull >> (64 - step));
                auto notNullCount = engine::popcount(mask);
                if (applyRestriction && notNullCount) {
                    auto* values = (*curPage)->template getData<uint32_t>() + nonNullIndex;
                    uint64_t newMask = applyRestriction->runDense(values, notNullCount);
                    mask = pdep(newMask, notNulls);
                }
#ifndef NDEBUG
                auto other = peek64(~0ull >> (64 - step), step);
                assert(mask == other);
#endif
                if (mask)
                    return {mask, skipped};
                skipped += step;
                tupleIndex += step;
                nonNullIndex += notNullCount;
                rowId += step;
            }
        }
        if (tupleIndex == (*curPage)->numRows) {
            curPage++;
            tupleIndex = 0;
            nonNullIndex = 0;
        }
        return {0, numTuples};
    }
};
//---------------------------------------------------------------------------
/// Make a reader given type
static UniquePtr<TableScan::Reader> makeReader(DataType type, DataSource::Page* const* curPage, DataSource::Page* const* endPage, const uint32_t* searchArray) {
    switch (type) {
        case DataType::INT32: return makeUnique<ReaderT<uint32_t>>(curPage, endPage, searchArray);
        case DataType::INT64: return makeUnique<ReaderT<uint64_t>>(curPage, endPage, searchArray);
        case DataType::FP64: return makeUnique<ReaderT<uint64_t>>(curPage, endPage, searchArray);
        case DataType::VARCHAR: return makeUnique<ReaderT<uint16_t>>(curPage, endPage, searchArray);
    }
    __builtin_unreachable();
}
//---------------------------------------------------------------------------
/// Make a restricted reader given type
static UniquePtr<TableScan::RestrictedReader> makeRestrictedReader(DataSource::Page* const* curPage, DataSource::Page* const* endPage, const uint32_t* searchArray, TableScan::RestrictionInfo func) {
    return makeUnique<TableScan::RestrictedReader>(curPage, endPage, searchArray, func);
}
//---------------------------------------------------------------------------
/// Constructor
TableScan::TableScan(TableInfo& table, const SmallVec<unsigned>& cols, const SmallVec<RestrictionInfo>& restrictions, double mult, double selectivity) : table(table) {
    UnorderedMap<unsigned, RestrictionInfo> colRestrictions;
    colRestrictions.reserve(restrictions.size());
    for (auto& r : restrictions) {
        assert(colRestrictions.find(r.column) == colRestrictions.end());
        colRestrictions[r.column] = r;
    }
    for (size_t c = 0; c < cols.size(); c++) {
        auto& col = cols[c];
        auto it = colRestrictions.find(col);
        RestrictionInfo myRest{0, 0.0, nullptr};
        if (it != colRestrictions.end()) {
            myRest = it->second;
            colRestrictions.erase(it);
        }
        if (myRest.restriction) {
            restrictedReaderDefs.emplace_back(true, readerDefs.size());
        }
        readerDefs.emplace_back(ReaderDef{col, myRest});
    }
    //// Some columns may need to be read only to apply the restrictions
    for (auto& [col, rest] : colRestrictions) {
        restrictedReaderDefs.emplace_back(false, nonOutputReaderDefs.size());
        nonOutputReaderDefs.emplace_back(ReaderDef{col, rest});
    }

    std::sort(restrictedReaderDefs.begin(), restrictedReaderDefs.end(), [&](auto& aindex, auto& bindex) -> bool {
        auto& a = std::get<0>(aindex) ? readerDefs[std::get<1>(aindex)].info : nonOutputReaderDefs[std::get<1>(aindex)].info;
        auto& b = std::get<0>(bindex) ? readerDefs[std::get<1>(bindex)].info : nonOutputReaderDefs[std::get<1>(bindex)].info;
        auto *arest = a.restriction, *brest = b.restriction;

        auto asel = a.selectivity, bsel = b.selectivity;
        if (asel == bsel) {
            asel = arest->estimateSelectivity();
            bsel = brest->estimateSelectivity();
        }
        auto acost = arest->estimateCost(), bcost = brest->estimateCost();

        return (1 - asel) / acost > (1 - bsel) / bcost;
    });

    if (!restrictedReaderDefs.empty()) {
        auto& [output, index] = restrictedReaderDefs[0];
        firstColumn = output ? readerDefs[index].column : nonOutputReaderDefs[index].column;
    } else {
        assert(!readerDefs.empty());
        firstColumn = readerDefs[0].column;
    }

    {
        morselEnd = table.numRows;
        auto& firstPages = table.columns[firstColumn]->pages;
        auto pageSize = (firstPages.empty() || firstPages[0]->getContainedRows() == 0) ? 1984 : firstPages[0]->getContainedRows();
        morselSize = table.numRows / Scheduler::concurrency();
        if (morselSize < 256) {
            morselSize = 256;
        } else if (morselSize > pageSize) {
            morselSize = std::min<size_t>(4, (morselSize + pageSize - 1) / pageSize) * pageSize;
            if (mult >= 2 && selectivity >= 0.1 && morselSize > pageSize) {
                morselSize = std::max( morselSize/ 2, pageSize);
                if (mult >= 4 && morselSize > pageSize)
                    morselSize = std::max( morselSize/ 2, pageSize);
            }
            assert(morselSize >= pageSize);
        }
    }
}
//---------------------------------------------------------------------------
TableScan::LocalState::LocalState(TableScan& scan) {
    readers.reserve(scan.readerDefs.size());
    nonOutputReaders.reserve(scan.nonOutputReaderDefs.size());

    for (auto& [c, f] : scan.readerDefs) {
        auto& col = *scan.table.columns[c];
        if (f.restriction)
            readers.emplace_back(makeRestrictedReader(col.pages.data(), col.pages.data() + col.pages.size(), col.pageOffsets, f));
        else
            readers.emplace_back(makeReader(col.type, col.pages.data(), col.pages.data() + col.pages.size(), col.pageOffsets));
    }
    for (auto& [c, f] : scan.nonOutputReaderDefs) {
        assert(f.restriction);
        assert(c < scan.table.columns.size());
        auto& col = *scan.table.columns[c];
        nonOutputReaders.emplace_back(makeRestrictedReader(col.pages.data(), col.pages.data() + col.pages.size(), col.pageOffsets, f));
    }
    values.resize((readers.size() + (scan.produceConstantColumn != ~0ull)) * bufferCount + bufferCount);
    valueIndex = 0;
    // To avoid undefined behaviour pointer addition
    if (values.empty())
        values.resize(1);
    if (scan.produceConstantColumn != ~0ull)
        std::fill(values.begin() + readers.size() * bufferCount, values.end(), scan.produceConstantColumn);

    restrictedReaders.reserve(scan.restrictedReaderDefs.size());
    for (auto& [output, index] : scan.restrictedReaderDefs) {
        if (output)
            restrictedReaders.push_back(static_cast<RestrictedReader*>(readers[index].get()));
        else
            restrictedReaders.push_back(static_cast<RestrictedReader*>(nonOutputReaders[index].get()));
    }

    allReaders.reserve(readers.size() + nonOutputReaders.size());
    for (auto& reader : readers)
        allReaders.push_back(reader.get());
    for (auto& reader : nonOutputReaders)
        allReaders.push_back(reader.get());
}
//---------------------------------------------------------------------------
TableScan::LocalState::LocalState(LocalState&&) noexcept = default;
TableScan::LocalState::~LocalState() noexcept = default;
//---------------------------------------------------------------------------
[[gnu::noinline]] uint32_t* makeSearchVector(engine::span<DataSource::Page* const> pages) {
    auto* res = static_cast<uint32_t*>(querymemory::allocate((pages.size() + 1) * sizeof(uint32_t)));
    res[0] = 0;
    {
        uint64_t total = 0;
        uint32_t* cur = res + 1;
        uint32_t* end = res + pages.size() + 1;
        auto* curPage = pages.data();
        for (; cur < end - 3; cur += 4, curPage += 4) {
            auto rows0 = curPage[0]->getContainedRows();
            auto rows1 = curPage[1]->getContainedRows();
            auto rows2 = curPage[2]->getContainedRows();
            auto rows3 = curPage[3]->getContainedRows();
            total += rows0;
            cur[0] = total;
            total += rows1;
            cur[1] = total;
            total += rows2;
            cur[2] = total;
            total += rows3;
            cur[3] = total;
        }
        for (; cur < end; cur++, curPage++) {
            total += curPage[0]->getContainedRows();
            cur[0] = total;
        }
    }
    return res;
}
//---------------------------------------------------------------------------
TableScan::ColumnInfo TableScan::prepareColumn(size_t numRows, DataSource::Column& column) {
    ColumnInfo result;
    result.type = column.type;
    result.pages = column.pages;
    result.pageOffsets = makeSearchVector(column.pages);
    return result;
}
//---------------------------------------------------------------------------
static SmallVec<unsigned> allColumns(const TableScan::TableInfo& table) {
    SmallVec<unsigned> cols;
    for (unsigned i = 0; i < table.columns.size(); i++)
        cols.push_back(i);
    return cols;
}
//---------------------------------------------------------------------------
TableScan::TableInfo TableScan::makeTableInfo(DataSource::Table& table) {
    TableScan::TableInfo info;
    info.numRows = table.numRows;
    info.columns.reserve(table.columns.size());
    info.columnsStorage.reserve(table.columns.size());
    for (auto& col : table.columns) {
        info.columnsStorage.push_back(TableScan::prepareColumn(table.numRows, col));
        info.columns.push_back(&info.columnsStorage.back());
    }
    info.name = table.name;
    return info;
}
//---------------------------------------------------------------------------
/// Constructor
TableScan::TableScan(TableInfo& table) : TableScan(table, allColumns(table), {}, 1, 1) {}
//---------------------------------------------------------------------------
/// Destructor
TableScan::~TableScan() noexcept = default;
//---------------------------------------------------------------------------
size_t TableScan::concurrency() const {
    if (morselEnd <= morselSize)
        return 1;
    return Scheduler::concurrency();
}
//---------------------------------------------------------------------------
void TableScan::produceImpl(FunctionRef<std::pair<void*, LocalState*>(size_t workerId)> getLocalState, FunctionRef<void(void* localStates, const uint64_t* values, const uint64_t* valuesEnd)> callback, FunctionRef<void(size_t workerId, void* localStateRaw, bool isInit)> init) {
    using namespace std;

    auto logic = [&](size_t workerId, size_t row) __attribute__((noinline)) {
        auto [lsRaw, ls] = getLocalState(workerId);
        if (row == ~0ull - 1) {
            init(workerId, lsRaw, true);
            return;
        }
        if (row == ~0ull) {
            ls->~LocalState();
            init(workerId, lsRaw, false);
            return;
        }

        const size_t end = std::min(row + morselSize, static_cast<size_t>(table.numRows));
        auto& readers = ls->readers;
        auto& allReaders = ls->allReaders;
        auto& restrictedReaders = ls->restrictedReaders;
        auto& nonOutputReaders = ls->nonOutputReaders;
        auto& values = ls->values;
        auto& valueIndex = ls->valueIndex;

        for (auto* reader : allReaders)
            assert(reader->rowId == allReaders[0]->rowId);

        for (size_t i = 0; i < allReaders.size(); i++)
            allReaders[i]->skipTo(row);

        while (row < end) {
            size_t maxTuples = end - row;
            for (auto* reader : allReaders) {
                while ((*reader->curPage)->numRows == 0 && !reader->done()) {
                    reader->curPage++;
                }
                auto rem = (*reader->curPage)->getContainedRows() - reader->tupleIndex;
                assert(rem > 0);
                maxTuples = std::min(maxTuples, rem);
            }

            assert(maxTuples);
            uint64_t mask = ~0ull >> (64 - std::min<size_t>(maxTuples, 64));
            if (!restrictedReaders.empty()) {
                size_t skipped = 0;
                while (skipped < maxTuples) {
                    size_t newSkipped = 0;
                    tie(mask, newSkipped) = restrictedReaders[0]->peekFirst(maxTuples - skipped);
                    skipped += newSkipped;

                    auto count = std::min<size_t>(maxTuples - skipped, 64);
                    if (mask) {
                        for (size_t r = 1; r < restrictedReaders.size(); r++) {
                            auto* reader = restrictedReaders[r];
                            if (reader->rowId < restrictedReaders[0]->rowId)
                                reader->skipMany(restrictedReaders[0]->rowId - reader->rowId);
                            mask = reader->peek64(mask, count);
                            if (!mask) {
                                restrictedReaders[0]->skipMany(count);
                                break;
                            }
                        }
                        if (mask)
                            break;
                    }
                    skipped += count;
                }

                if (skipped) {
                    for (auto* reader : allReaders) {
                        if (reader->rowId < restrictedReaders[0]->rowId)
                            reader->skipMany(restrictedReaders[0]->rowId - reader->rowId);
                    }
                    row += skipped;
                    maxTuples -= skipped;
                    for (auto* reader : allReaders)
                        assert(reader->rowId == row);
                }
            }

            maxTuples = std::min<size_t>(maxTuples, 64);
            assert(!mask || maxTuples);
            if (maxTuples) {
                assert(mask);
                uint64_t* target = values.data() + valueIndex;
                for (auto& reader : readers) {
                    assert(reader->rowId == row);
                    reader->step64(target, mask, maxTuples);
                    assert(target + maxTuples <= values.data() + values.size());
                    target += bufferCount;
                }
                for (auto& reader : nonOutputReaders) {
                    assert(reader->rowId == row);
                    reader->skipMany(maxTuples);
                }

                auto* v = values.data();
                auto c = engine::popcount(mask);
                valueIndex += c;
                if (valueIndex >= bufferCount / 2) {
                    callback(lsRaw, v, v + valueIndex);
                    valueIndex = 0;
                }

                row += maxTuples;
            }
            for (auto* reader : allReaders)
                assert(reader->rowId == row);
        }

        if (valueIndex) {
            callback(lsRaw, values.data(), values.data() + valueIndex);
            valueIndex = 0;
        }
    };
    if (concurrency() <= 1) {
        logic(0, ~0ull - 1);
        size_t row = 0;
        while (row < morselEnd) {
            logic(0, row);
            row += morselSize;
        }
        logic(0, ~0ull);
    } else {
        Scheduler::parallelMorsel(0, morselEnd, morselSize, logic, true);
    }
}
//---------------------------------------------------------------------------
Vector<uint32_t> TableScan::createUnfilteredSample(size_t sampleSize) const {
    SmallVec<ReaderT<uint32_t>> readers;
    for (auto& [c, f] : readerDefs) {
        auto& col = *table.columns[c];
        readers.emplace_back(col.pages.data(), col.pages.data() + col.pages.size(), col.pageOffsets);
    }

    assert(sampleSize <= table.numRows);
    Random rng;
    SmallVec<uint64_t> rowIds;
    rowIds.reserve(sampleSize);
    for (size_t i = 0; i < sampleSize; i++)
        rowIds.push_back(rng.nextRange(table.numRows));
    std::sort(rowIds.begin(), rowIds.end());

    Vector<uint32_t> result(sampleSize * readers.size());

    for (size_t ind = 0; ind < rowIds.size(); ind++) {
        auto rowId = rowIds[ind];
        for (auto& reader : readers)
            reader.skipTo(rowId);
        for (size_t col = 0; col < readers.size(); col++) {
            auto& reader = readers[col];
            bool isNull = (*reader.curPage)->isNull(reader.tupleIndex);
            result[ind + col * sampleSize] = isNull ? static_cast<uint32_t>(RuntimeValue::nullValue) : (*reader.curPage)->template get<uint32_t>(reader.nonNullIndex);
        }
    }

    return result;
}
//---------------------------------------------------------------------------
std::string_view TableScan::getTableName() const noexcept {
    std::string_view res{table.name};
    // Take res until first '|'
    auto pos = res.find('|');
    if (pos != std::string_view::npos)
        res = res.substr(0, pos);
    return res;
}
//---------------------------------------------------------------------------
std::string TableScan::getPretty() const {
    return "";
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
