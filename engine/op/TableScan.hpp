#pragma once
//---------------------------------------------------------------------------
#include "infra/QueryMemory.hpp"
#include "infra/SmallVec.hpp"
#include "infra/Util.hpp"
#include "infra/helper/Span.hpp"
#include "op/ScanBase.hpp"
#include "query/DataSource.hpp"
#include "query/Restriction.hpp"
#include <memory>
#include <optional>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class QueryMemory;
class RestrictionLogic;
//---------------------------------------------------------------------------
class TableScan : public ScanImpl<TableScan> {
    public:
    static constexpr uint64_t nullValue = Restriction::nullValue;

    /// Reader for a column
    struct Reader;
    struct RestrictedReader;
    /// The column readers
    struct LocalState {
        SmallVec<UniquePtr<Reader>> readers;
        SmallVec<UniquePtr<Reader>> nonOutputReaders;
        SmallVec<uint64_t> values;
        SmallVec<RestrictedReader*> restrictedReaders;
        SmallVec<Reader*> allReaders;
        /// The current index within values
        size_t valueIndex = 0;

        LocalState(TableScan& scan);
        ~LocalState() noexcept;
        LocalState(LocalState&&) noexcept;
    };

    /// The columns we want to scan
    struct ColumnInfo {
        /// The data type of the column
        DataType type;
        /// The constained pages
        engine::span<DataSource::Page* const> pages;
        /// The page offset prefix sum for accelerating scans
        uint32_t* pageOffsets;
    };

    /// Prepare column information
    static ColumnInfo prepareColumn(size_t numRows, DataSource::Column& column);

    /// The pages we want to scan
    struct TableInfo {
        /// The rows
        uint64_t numRows;
        /// The columns
        SmallVec<const ColumnInfo*> columns;
        /// The name for debugging
        std::string name;
        /// Optional storage for columns
        SmallVec<ColumnInfo> columnsStorage;

        ~TableInfo() {
            columns = {};
            columnsStorage = {};
        }
    };

    /// Make table info from table. For tests
    static TableInfo makeTableInfo(DataSource::Table& table);

    /// Info on restriction
    struct RestrictionInfo {
        unsigned column;
        double selectivity;
        const RestrictionLogic* restriction;
    };

    private:
    struct ReaderDef {
        unsigned column;
        RestrictionInfo info;
    };
    /// The actual table
    TableInfo& table;
    /// The definitions for readers
    SmallVec<ReaderDef> readerDefs;
    /// The definitions for nonOutputReaders
    SmallVec<ReaderDef> nonOutputReaderDefs;
    /// The restricted readers
    SmallVec<std::tuple<bool, unsigned>> restrictedReaderDefs;
    /// The first column of the table
    unsigned firstColumn = 0;
    size_t morselEnd = 0;
    size_t morselSize = 0;

    /// Number of tuples in buffer
    static constexpr size_t bufferCount = 128;

    /// Implementation for produce
    void produceImpl(FunctionRef<std::pair<void*, LocalState*>(size_t workerId)> getLocalState, FunctionRef<void(void* localStateRaw, const uint64_t* values, const uint64_t* valuesEnd)> callback, FunctionRef<void(size_t workerId, void* localStateRaw, bool isInit)> init);

    public:
    /// Produce an additional final column just containing a constant value
    uint64_t produceConstantColumn = ~0ull;

    /// Constructor
    TableScan(TableInfo& table, const SmallVec<unsigned>& cols, const SmallVec<RestrictionInfo>& restrictions, double mult, double selectivity);
    /// Constructor
    explicit TableScan(TableInfo& table);
    /// Destructor
    ~TableScan() noexcept;

    /// Get the number of produced columns
    size_t getProducedColumns() const noexcept { return readerDefs.size(); }

    /// Produce all tuples
    template <typename LS, typename Consume, typename Prepare, typename Init>
    void operator()(LS&& localStateFun, Consume&& consume, Prepare&& prepare, Init&& init) {
        produceImpl(
            [&](size_t workerId) __attribute__((noinline)) -> std::pair<void*, LocalState*> {
                return {localStateFun(workerId), &localStateFun(workerId)->scan};
            },
            [&](void* localStateRaw, const uint64_t* values, const uint64_t* valuesEnd) __attribute__((noinline)) {
                for (; values != valuesEnd; values += 1) {
                    prepare(localStateRaw, (values + 1)[0]);
                    consume(localStateRaw, [ptr = values](unsigned ind) { return ptr[ind * bufferCount]; });
                }
            },
            [&](size_t workerId, void* localStateRaw, bool isInit) {
                init(workerId, localStateRaw, isInit);
            });
    }
    std::string_view getTableName() const noexcept;
    std::string getPretty() const override;
    size_t concurrency() const override;
    /// Create a random sample of size without restrictions (only integer columns)
    Vector<uint32_t> createUnfilteredSample(size_t sampleSize) const;
};
//---------------------------------------------------------------------------
static_assert(ScanOperator<TableScan>);
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
