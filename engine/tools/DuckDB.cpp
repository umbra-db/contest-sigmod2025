#include "tools/DuckDB.hpp"
#ifndef NO_DUCK
#include <duckdb.hpp>
#endif
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct DuckDB::Impl {
#ifndef NO_DUCK
    duckdb::DBConfig config;
    duckdb::DuckDB db;
    duckdb::Connection conn;

    Impl() : config(true), db("imdb.db", &config), conn(db) {
        conn.Query("SET memory_limit = '20GB';");
        conn.Query("SET temp_directory = '';");
    }
#endif
};
//---------------------------------------------------------------------------
#ifndef NO_DUCK
static DataType mapType(const duckdb::LogicalType& lhs) {
    using namespace duckdb;
    switch (lhs.id()) {
        case LogicalTypeId::INTEGER: return DataType::INT32;
        case LogicalTypeId::BIGINT:  return DataType::INT64;
        case LogicalTypeId::DOUBLE:  return DataType::FP64;
        case LogicalTypeId::VARCHAR: return DataType::VARCHAR;
        default:
            throw std::runtime_error("in DuckDB is not supported");
    }
}
#endif
//---------------------------------------------------------------------------
DuckDB::DuckDB() : impl(std::make_unique<Impl>()) {}
DuckDB::~DuckDB() noexcept = default;
//---------------------------------------------------------------------------
ColumnarTable DuckDB::execute(std::string query) {
#ifndef NO_DUCK
    auto results = impl->conn.SendQuery(query);
    auto& duckdb_results = *results;
    auto num_cols = duckdb_results.ColumnCount();

    std::vector<DataType> cols;

    for (size_t i = 0; i < num_cols; ++i) {
        cols.push_back(mapType(duckdb_results.types[i]));
    }

    std::vector<std::vector<Data>> duckdb_table;
    size_t rowCount = 0;
    for (auto& row : *results) {
        if (rowCount > 50'000'000ull) {
            throw std::runtime_error("Too many rows in result");
        }
        rowCount++;
        std::vector<Data> record;
        for (size_t col_idx = 0; col_idx < num_cols; col_idx++) {
            auto val = row.iterator.chunk->GetValue(col_idx, row.row);
            if (val.IsNull()) {
                record.emplace_back(std::monostate{});
            } else {
                switch (cols[col_idx]) {
                    case DataType::INT32: {
                        record.emplace_back(duckdb::IntegerValue::Get(val));
                        break;
                    }
                    case DataType::INT64: {
                        record.emplace_back(duckdb::BigIntValue::Get(val));
                        break;
                    }
                    case DataType::FP64: {
                        record.emplace_back(duckdb::FloatValue::Get(val));
                        break;
                    }
                    case DataType::VARCHAR: {
                        record.emplace_back(duckdb::StringValue::Get(val));
                        break;
                    }
                }
            }
        }
        duckdb_table.emplace_back(std::move(record));
    }
    sort(duckdb_table.begin(), duckdb_table.end());
    Table tbl(std::move(duckdb_table), std::move(cols));
    return tbl.to_columnar();
#else
    throw std::runtime_error("Built with NO_DUCK but trying to run a DuckDB query!");
#endif
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------