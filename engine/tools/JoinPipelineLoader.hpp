#pragma once
//---------------------------------------------------------------------------
#include <nlohmann/json_fwd.hpp>
#include <plan.h>
#include <table_entity.h>
#include <table.h>
#include <unordered_set>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct ParsedSQL;
class DataSource;
using OutputAttrsType = std::vector<std::tuple<TableEntity, std::string>>;
//---------------------------------------------------------------------------
struct DataSourceBuilder {
    DataSource& db;
    std::vector<ColumnarTable> columns;
    std::unordered_map<std::string, uint32_t> tables;
};
//---------------------------------------------------------------------------
struct JoinPipelineLoader {
    const ParsedSQL& parsed_sql;
    DataSourceBuilder& db;
    ::Plan ret;

    std::unordered_set<TableEntity> extract_entities(const nlohmann::json& node);
    std::tuple<size_t, std::vector<std::tuple<TableEntity, std::string, DataType>>> recurse(const nlohmann::json& node, const OutputAttrsType& required_attrs);

    static ::Plan load_join_pipeline(DataSourceBuilder& db, const nlohmann::json& node, const ParsedSQL& parsed_sql);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------