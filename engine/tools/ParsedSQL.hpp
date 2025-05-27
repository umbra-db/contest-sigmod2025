#pragma once
//---------------------------------------------------------------------------
#include <nlohmann/json_fwd.hpp>
#include <plan.h>
#include <table_entity.h>
#include <tuple>
#include <vector>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
using OutputAttrsType = std::vector<std::tuple<TableEntity, std::string>>;
using AliasMapType = std::unordered_map<std::string, TableEntity>;
using FilterMapType = std::unordered_map<TableEntity, std::unique_ptr<Statement>>;
using JoinGraphType = std::unordered_map<TableEntity,
                                         std::unordered_map<TableEntity, std::tuple<std::string, std::string>>>;
using ColumnMapType = std::unordered_map<TableEntity, std::unordered_map<std::string, size_t>>;
//---------------------------------------------------------------------------
struct ParsedSQL {
    static const std::unordered_map<std::string, std::vector<Attribute>> attributes_map;

    const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables;
    std::unordered_map<std::string, int> table_counts;
    AliasMapType alias_map;
    std::unordered_map<TableEntity, std::string> entity_to_alias;
    JoinGraphType join_graph;
    FilterMapType filters;
    OutputAttrsType output_attrs;
    ColumnMapType column_map;
    std::vector<std::tuple<TableEntity, std::string>> column_vec;

    ParsedSQL(const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables);

    std::string executed_sql(const std::string& sql);

    void parse_sql(const std::string& sql, std::string_view name);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------