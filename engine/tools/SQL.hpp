#pragma once
//---------------------------------------------------------------------------
#include "query/DataSource.hpp"
#include "query/QueryPlan.hpp"
#include <vector>
#include <string>
#include <unordered_map>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// Parsing SQL and plans
class SQL {
    public:
    /// Plan maker
    struct PlanMaker {
        virtual ~PlanMaker() noexcept = default;
        virtual QueryPlan makePlan() = 0;
    };
    /// The parse result
    struct Query {
        /// Name of the query
        std::string name;
        /// The SQL query for duckdb
        std::string sql;
        /// The plan for the query
        std::unique_ptr<PlanMaker> planMaker;
        /// The index for the DuckDB result relation
        unsigned resultRelation;
    };
    /// Batch of queries
    struct Batch {
        /// The data
        std::unique_ptr<DataSource> db;
        /// The queries
        std::vector<Query> queries;
    };
    /// Parse all queries
    static Batch parse(const std::string& planFile, std::vector<std::string> selected);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------