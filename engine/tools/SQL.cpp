#include "query/PlanImport.hpp"
#include "tools/ParsedSQL.hpp"
#include "tools/JoinPipelineLoader.hpp"
#include "tools/SQL.hpp"
#include "tools/DuckDB.hpp"
#include "tools/Setting.hpp"
#pragma GCC push_options
#pragma GCC optimize("O3")
#include <nlohmann/json.hpp>
#pragma GCC pop_options
#include <common.h>
#include <unordered_set>
//---------------------------------------------------------------------------
using json = nlohmann::json;
namespace fs = std::filesystem;
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// Should we serialize the output
static Setting serialize("SERIALIZE", setting::Bool(true));
//---------------------------------------------------------------------------
SQL::Batch SQL::parse(const std::string& planFile, std::vector<std::string> selected) {
    std::unordered_map<std::string, std::vector<std::string>> column_to_tables;

    for (auto& [table_name, attributes] : ParsedSQL::attributes_map) {
        for (auto& [_, attribute_name] : attributes) {
            if (auto itr = column_to_tables.find(attribute_name);
                itr == column_to_tables.end()) {
                column_to_tables.emplace(attribute_name, std::vector<std::string>{table_name});
            } else {
                itr->second.push_back(table_name);
            }
        }
    }

    File file(planFile, "rb");
    json query_plans = json::parse(file);
    Batch batch;
    batch.db = std::make_unique<DataSource>();
    auto sql_directory = query_plans["sql_directory"].get<std::string>();
    auto names = query_plans["names"].get<std::vector<std::string>>();
    auto plans = query_plans["plans"];

    DuckDB duckdb;

    std::unordered_set<std::string> selected_plans(selected.begin(), selected.end());

    if (std::filesystem::exists(".cache.db")) {
        *batch.db = DataSource::deserialize(".cache.db");
    }

    DataSourceBuilder dbb{*batch.db};
    for (size_t i = 0; i < batch.db->relations.size(); i++)
        dbb.tables[batch.db->relations[i].name] = i;

    fmt::print("Loading queries\n");
    auto nameIter = names.begin();
    auto plansIter = plans.begin();
    for (; nameIter != names.end() && plansIter != plans.end(); ++nameIter, ++plansIter) {
        auto& name = *nameIter;
        auto& plan_json = *plansIter;
        if (selected_plans.empty() || (selected_plans.find(name) != selected_plans.end())) {
            auto sql_path = fs::path(sql_directory) / fmt::format("{}.sql", name);
            auto sql = read_file(sql_path);

            ParsedSQL parsed_sql(column_to_tables);
            parsed_sql.parse_sql(sql, name);
            auto executed = parsed_sql.executed_sql(sql);

            Plan plan;
            try {
                plan = JoinPipelineLoader::load_join_pipeline(dbb, plan_json["Plan"], parsed_sql);
            } catch (const std::exception& e) {
                fmt::print("SKIPPING: Could not load plan for query {} with reason: {}\n", name, e.what());
                continue;
            }

            auto resultName = DataSource::Table::fixName(name + "||result");
            if (dbb.tables.find(resultName) == dbb.tables.end()) {
                fmt::print("DuckDB result for {} not found, computing\n", name);
                ColumnarTable res;
                try {
                    res = duckdb.execute(executed);
                } catch (const std::exception& e) {
                    fmt::print("SKIPPING: DuckDB could not compute result for query {} with reason: {}\n", name, e.what());
                    continue;
                }
                dbb.columns.push_back(std::move(res));
                DataSource::Table tbl = PlanImport::importTable(dbb.columns.back());
                tbl.name = resultName;
                dbb.db.relations.push_back(std::move(tbl));
                dbb.tables[resultName] = dbb.columns.size() - 1;
            }

            struct Info final : public PlanMaker {
                DataSource* db = nullptr;
                Plan plan;
                Info(DataSource* db, Plan plan) : db(db), plan(std::move(plan)) {}
                QueryPlan makePlan() override {
                    return PlanImport::importPlanExistingData(*db, plan);
                }
            };
            std::unique_ptr<PlanMaker> info = std::make_unique<Info>(batch.db.get(), std::move(plan));
            batch.queries.push_back(Query{name, executed, std::move(info), dbb.tables.at(resultName)});
        }
    }

    if (serialize.get() && !dbb.columns.empty()) {
        fmt::print("Serializing newly loaded data\n");
        std::move(*batch.db).serialize(".cache.db");
        *batch.db = DataSource::deserialize(".cache.db");
    }
    fmt::print("Loaded queries\n");

    return batch;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
