#pragma once
//---------------------------------------------------------------------------
#include "query/QueryPlan.hpp"
#include <memory>
//---------------------------------------------------------------------------
struct Plan;
struct ColumnarTable;
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct PlanImport {
    static QueryPlan importPlanExistingData(DataSource& dataSource, const Plan& plan);
    static QueryPlan importPlan(DataSource& dataSource, const Plan& plan);

    static DataSource::Table importTable(const ColumnarTable& tbl);

    using Data = std::variant<int32_t, int64_t, double, std::string, std::monostate>;
    /// Used for testing
    struct TableResult {
        virtual ~TableResult() = default;
        DataSource::Table table;
    };
    /// Used for testing
    static std::unique_ptr<TableResult> makeTable(std::vector<std::vector<Data>> data, std::vector<DataType> types);
};
//---------------------------------------------------------------------------
}