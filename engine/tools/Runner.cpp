#include "Execute.hpp"
#include "infra/Scheduler.hpp"
#include "op/TableScan.hpp"
#include "query/PlanImport.hpp"
#include "storage/StringPtr.hpp"
#include "table.h"
#include "tools/SQL.hpp"
#include "tools/Setting.hpp"
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <vector>
//---------------------------------------------------------------------------
using Result = std::tuple<bool, size_t, std::string_view>;
//---------------------------------------------------------------------------
static engine::Setting repeats("REPEAT", engine::setting::Size(10));
static engine::Setting diff_count("DIFF.count_to_print", engine::setting::Size(20));
static engine::Setting fastCompare("FASTCOMP", engine::setting::Bool(false));
static engine::Setting checkResult("CHECKRESULT", engine::setting::Bool(true));
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
void sort(std::vector<std::vector<Data>>& table) {
    std::sort(table.begin(), table.end());
}
//---------------------------------------------------------------------------
using namespace engine;
template <typename _To, typename _From>
[[nodiscard]] constexpr _To bit_cast_helper(const _From& __from) noexcept {
    return __builtin_bit_cast(_To, __from);
}
//---------------------------------------------------------------------------
struct TypedCollector {
    engine::span<const DataType> types;
    std::vector<std::vector<PlanImport::Data>> values = {};
    std::mutex values_mutex;
    std::atomic<size_t> diff_ctr = 0;

    static PlanImport::Data convert(uint64_t val, DataType type) {
        if (val == TableScan::nullValue)
            return std::monostate{};
        switch (type) {
            case DataType::INT32: return int32_t(val);
            case DataType::INT64: return int64_t(val);
            case DataType::FP64: return bit_cast_helper<double>(val);
            case DataType::VARCHAR: {
                auto str = StringPtr{val};
                return str.materialize_string();
            }
        }
        __builtin_unreachable();
    }

    template <typename T, typename ProviderT>
    void operator()(T&, ProviderT provider) {
        bool validResult = true;
        std::vector<PlanImport::Data> row;
        row.resize(types.size());
        size_t right_offset = types.size() / 2;
        for (size_t i = 0; i < right_offset; i++) {
            auto v1 = convert(provider(i), types[i]);
            auto v2 = convert(provider(i + right_offset), types[i + right_offset]);
            row[i] = v1;
            row[i + right_offset] = v2;

            validResult &= (v1 == v2);
        }
        if (!validResult && ++diff_ctr < diff_count.get()) {
            std::scoped_lock lock(values_mutex);
            values.emplace_back(row);
        }
    }

    void printDiff() const {
        auto diff = diff_ctr.load();
        assert(diff != 0);

        for (auto& curVal : values) {
            size_t right_offset = types.size() / 2;
            std::cout << "(";
            for (size_t i = 0; i < curVal.size(); ++i) {
                std::string separator;
                if (i == right_offset - 1) {
                    separator = ") vs. (";
                } else if (i == types.size() - 1) {
                    separator = ")\n";
                } else {
                    separator = ";";
                }
                if (std::holds_alternative<std::monostate>(curVal[i])) {
                    std::cout << "<null>" << separator;
                } else {
                    switch (types[i]) {
                        case DataType::INT32: std::cout << std::get<int32_t>(curVal[i]) << separator; break;
                        case DataType::INT64: std::cout << std::get<int64_t>(curVal[i]) << separator; break;
                        case DataType::FP64: std::cout << std::get<double>(curVal[i]) << separator; break;
                        case DataType::VARCHAR: std::cout << "'" << std::get<std::string>(curVal[i]) << "'" << separator; break;
                    }
                }
            }
        }
        if (diff - values.size() != 0)
            std::cout << "... and " << diff - values.size() << " other rows.\n";
        std::cout << std::flush;
    }
};
//---------------------------------------------------------------------------
static bool compare(engine::DataSource::Table& duckdb_results, const ColumnarTable& results) {
    auto num_rows = results.num_rows;
    auto num_cols = results.columns.size();
    auto compare_result = results.num_rows == duckdb_results.numRows;
    assert(compare_result && "Wrong row count in result!");

    compare_result &= (duckdb_results.columns.size() == results.columns.size());
    assert(compare_result && "Wrong number of columns in result!");

    for (size_t i = 0; i < num_cols; ++i) {
        compare_result &= (duckdb_results.columns[i].type == results.columns[i].type);
        assert(compare_result && "Wrong column types!");
    }

    for (size_t colIdx = 0; colIdx < results.columns.size(); colIdx++) {
        auto& col = results.columns[colIdx];
        uint64_t colSize = 0;
        for (auto& page : col.pages) {
            uint16_t tupleCount;
            memcpy(&tupleCount, page->data, sizeof(uint16_t));
            colSize += tupleCount >= 0xfffe ? tupleCount - 0xfffe : tupleCount;
        }
        assert(colSize == duckdb_results.numRows && "Wrong number of tuples in column");
        compare_result = compare_result && (colSize == duckdb_results.numRows);
    }
    assert(compare_result);
    if (fastCompare.get())
        return compare_result;

    auto result_table = Table::from_columnar(results);
    sort(result_table.table());
    auto result_table_columnar = result_table.to_columnar();

    engine::DataSource::Table combinedTable = duckdb_results;
    std::vector<DataType> types;
    for (const auto& column : duckdb_results.columns)
        types.emplace_back(column.type);
    for (const auto& column : result_table_columnar.columns) {
        DataSource::Column c;
        c.type = column.type;
        c.pages = engine::span(reinterpret_cast<DataSource::Page* const*>(column.pages.data()), column.pages.size());
        combinedTable.columns.push_back(c);
        types.emplace_back(column.type);
    }

    auto tblInfo = engine::TableScan::makeTableInfo(combinedTable);
    engine::TableScan scan(tblInfo);
    TypedCollector collector{types};

    struct LS {
        TableScan::LocalState scan;
        LS(TableScan& scan) : scan(scan) {}
    };
    auto initLS = [](TableScan& scan) {
        std::vector<LS> ls;
        unsigned sz = Scheduler::concurrency();
        ls.reserve(sz);
        for (unsigned i = 0; i < sz; i++)
            ls.emplace_back(scan);
        return ls;
    };

    auto ls = initLS(scan);
    scan([&ls](size_t workerId) { return &ls[workerId]; }, collector, [](auto, uint64_t) {}, [](auto, auto, auto) {});

    if (collector.diff_ctr.load() != 0) {
        std::cout << "Detected difference in result: should vs. actual";
        collector.printDiff();
    }
    assert(collector.diff_ctr.load() == 0);
    compare_result &= (collector.diff_ctr.load() == 0);
    assert(compare_result);

    return compare_result;
}
//---------------------------------------------------------------------------
} // namespace
//---------------------------------------------------------------------------
static std::tuple<bool, size_t, std::string_view> run(engine::DataSource& db, engine::SQL::Query& query, [[maybe_unused]] void* context) {
    fmt::print("\rRunning query:  {}         ", query.name);
    fflush(stdout);
    Scheduler::start_query();

    auto rpts = repeats.get();

    auto start = std::chrono::steady_clock::now();
    ColumnarTable results;
    {
        for (size_t i = 0; i < rpts; i++) {
            results.columns = std::vector<Column>{};

            results = engine::execute(query.planMaker->makePlan(), context);
        }
    }
    auto end = std::chrono::steady_clock::now();

    fmt::print("\rChecking query: {}         ", query.name);
    fflush(stdout);

    auto compare_result = checkResult.get() && compare(db.relations[query.resultRelation], results);

    return {compare_result, std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / rpts, query.name};
}
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    const auto output_filename = std::string{"BENCHMARK_RUNTIME.txt"};
    const auto record_filename = std::string{"record.csv"};

    fmt::print("Using {} threads\n", engine::Scheduler::concurrency());

    void* context = nullptr;
    try {
        if (argc < 2) {
            // We accept several plans to run. For now, the last one of them might be `output_filename`  in which
            // case we write the runtime result to this file.
            fmt::println(stderr, "{} <path to plans> [<name of selected sql>] [{}/{}]", argv[0], output_filename, record_filename);
            exit(EXIT_FAILURE);
        }

        auto runtime = size_t{0};

        // Build context. Context creation time is included in the measured runtime.
        const auto start_context = std::chrono::steady_clock::now();
        context = Contest::build_context();
        const auto end_context = std::chrono::steady_clock::now();
        runtime += std::chrono::duration_cast<std::chrono::microseconds>(end_context - start_context).count();

        // load plan json

        auto write_output_file = false, write_record_file = false;
        auto selected_plans = std::vector<std::string>{};
        for (int argument_id = 2; argument_id < argc; ++argument_id) {
            if (argument_id == argc - 1 && std::string{argv[argument_id]} == output_filename) {
                write_output_file = true;
                break;
            } else if (argument_id == argc - 1 && std::string{argv[argument_id]} == record_filename) {
                write_record_file = true;
                break;
            }
            selected_plans.emplace_back(argv[argument_id]);
        }

        auto queries = engine::SQL::parse(argv[1], selected_plans);

        bool all_queries_succeeded = true;

        std::vector<Result> results;
        for (engine::SQL::Query& query : queries.queries)
            results.push_back(run(*queries.db, query, context));
        fmt::print("\n");

        std::sort(results.begin(), results.end(), [](auto& a, auto& b) { return std::get<1>(a) < std::get<1>(b); });

        unsigned i = 0;
        for (auto [result_is_correct, query_runtime, query_name] : results) {
            runtime += query_runtime;
            all_queries_succeeded &= result_is_correct;
            fmt::println("{:3}: Query {:3} >> \t\t Runtime: {:.2f} ms - Result{} correct: {} - Total runtime: {:.1f} ms",
                         ++i,
                         query_name,
                         static_cast<float>(query_runtime) / 1000.0f,
                         fastCompare.get() ? " size" : "",
                         result_is_correct,
                         runtime / 1000.0f);
        }

        fmt::println("All queries succeeded: {}", all_queries_succeeded);
        fmt::println("Total runtime: {:.2f} ms", runtime / 1000.0f);

        if (write_output_file) {
            auto output_file = std::ofstream(output_filename);
            output_file << runtime;
            output_file.close();
        }

        // destroy context
        Contest::destroy_context(context);

        return !all_queries_succeeded; // Get 0 for success, 1 for failure.
    } catch (std::exception& e) {
        // destroy context
        if (context)
            Contest::destroy_context(context);

        fmt::println(stderr, "Error: {}", e.what());
        exit(EXIT_FAILURE);
    }
}
