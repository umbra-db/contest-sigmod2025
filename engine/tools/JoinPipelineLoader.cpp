#include "tools/JoinPipelineLoader.hpp"
#include "tools/ParsedSQL.hpp"
#include "query/PlanImport.hpp"
#include "query/QueryPlan.hpp"
#include <table.h>
#pragma GCC push_options
#pragma GCC optimize("O3")
#include <nlohmann/json.hpp>
#pragma GCC pop_options
//---------------------------------------------------------------------------
using json = nlohmann::json;
namespace fs = std::filesystem;
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
static std::unordered_set<std::string_view> other_operators{"Aggregate", "Gather"};
static std::unordered_set<std::string_view> join_types{"Nested Loop",
                                                "Hash Join",
                                                "Merge Join"};
static std::unordered_set<std::string_view> scan_types{"Seq Scan", "Index Only Scan"};
//---------------------------------------------------------------------------
std::unordered_set<TableEntity> JoinPipelineLoader::extract_entities(const json& node) {
    auto& alias_map = parsed_sql.alias_map;
    auto node_type = node["Node Type"].get<std::string_view>();

    if (auto itr = other_operators.find(node_type); itr != other_operators.end()) {
        return extract_entities(node["Plans"][0]);
    } else if (auto itr = join_types.find(node_type); itr != join_types.end()) {
        if (node_type != "Hash Join") {
            throw std::runtime_error("Not Hash Join");
        }
        auto left_type = node["Plans"][0]["Node Type"].get<std::string_view>();
        auto right_type = node["Plans"][1]["Node Type"].get<std::string_view>();
        std::unordered_set<TableEntity> left_entities, right_entities;
        if (left_type == "Hash" and right_type != "Hash") {
            left_entities =
                extract_entities(node["Plans"][0]["Plans"][0]);
            right_entities = extract_entities(node["Plans"][1]);
        } else if (left_type != "Hash" and right_type == "Hash") {
            left_entities = extract_entities(node["Plans"][0]);
            right_entities =
                extract_entities(node["Plans"][1]["Plans"][0]);
        } else {
            throw std::runtime_error("Hash Join should have at least one Hash child");
        }
        left_entities.merge(std::move(right_entities));
        return left_entities;
    } else if (auto itr = scan_types.find(node_type); itr != scan_types.end()) {
        if (not node.contains("Alias")) {
            throw std::runtime_error("No \"Alias\" in scan node");
        }
        auto alias = node["Alias"].get<std::string>();
        TableEntity entity;
        if (auto itr = alias_map.find(alias); itr != alias_map.end()) {
            entity = itr->second;
        } else {
            throw std::runtime_error(fmt::format("Cannot find alias: {}", alias));
        }
        std::unordered_set<TableEntity> entities{std::move(entity)};
        return entities;
    } else {
        throw std::runtime_error(fmt::format("Not supported node type: {}", node_type));
    }
}
//---------------------------------------------------------------------------
std::tuple<size_t, std::vector<std::tuple<TableEntity, std::string, DataType>>> JoinPipelineLoader::recurse(const json& node, const OutputAttrsType& required_attrs) {
    auto& table_counts = parsed_sql.table_counts;
    auto& alias_map = parsed_sql.alias_map;
    auto& join_graph = parsed_sql.join_graph;
    auto& filters = parsed_sql.filters;

    auto node_type = node["Node Type"].get<std::string_view>();

    if (auto itr = other_operators.find(node_type); itr != other_operators.end()) {
        return recurse(node["Plans"][0], required_attrs);
    } else if (auto itr = join_types.find(node_type); itr != join_types.end()) {
        if (node_type != "Hash Join") {
            throw std::runtime_error("Not Hash Join");
        }
        auto left_type = node["Plans"][0]["Node Type"].get<std::string_view>();
        auto right_type = node["Plans"][1]["Node Type"].get<std::string_view>();
        bool build_left;
        size_t left, right;
        size_t left_attr, right_attr;
        std::unordered_set<TableEntity> left_entities, right_entities;
        std::vector<std::tuple<TableEntity, std::string, DataType>> left_columns,
            right_columns;
        TableEntity left_entity, right_entity;
        std::string left_column, right_column;
        const json* pleft;
        const json* pright;
        if (left_type == "Hash" and right_type != "Hash") {
            build_left = true;
            pleft = &node["Plans"][0]["Plans"][0];
            pright = &node["Plans"][1];
        } else if (left_type != "Hash" and right_type == "Hash") {
            build_left = false;
            pleft = &node["Plans"][0];
            pright = &node["Plans"][1]["Plans"][0];
        } else {
            throw std::runtime_error("Hash Join should have at least one Hash child");
        }
        left_entities = extract_entities(*pleft);
        right_entities = extract_entities(*pright);
        bool found_join_condition = false;
        for (auto& entity : left_entities) {
            if (auto itr = join_graph.find(entity); itr != join_graph.end()) {
                for (auto& [adj, columns] : itr->second) {
                    if (auto iter = right_entities.find(adj);
                        iter != right_entities.end()) {
                        left_entity = entity;
                        right_entity = adj;
                        std::tie(left_column, right_column) = columns;
                        found_join_condition = true;
                    }
                }
            }
        }
        if (not found_join_condition) {
            fmt::println(stderr, "left entities:");
            for (auto& entity : left_entities) {
                fmt::println(stderr, "    {}", entity);
            }
            fmt::println(stderr, "right entities:");
            for (auto& entity : right_entities) {
                fmt::println(stderr, "    {}", entity);
            }
            throw std::runtime_error("Cannot find join condition");
        }
        OutputAttrsType left_required, right_required;
        bool left_attr_already_in = false, right_attr_already_in = false;
        for (const auto& [required_entity, required_column] : required_attrs) {
            if (auto itr = left_entities.find(required_entity);
                itr != left_entities.end()) {
                if (required_entity == left_entity and required_column == left_column) {
                    left_attr_already_in = true;
                }
                left_required.emplace_back(required_entity, required_column);
            } else if (auto itr = right_entities.find(required_entity);
                       itr != right_entities.end()) {
                if (required_entity == right_entity and required_column == right_column) {
                    right_attr_already_in = true;
                }
                right_required.emplace_back(required_entity, required_column);
            } else {
                throw std::runtime_error(
                    "Required attributes cannot be found in neither left child nor right "
                    "child.");
            }
        }
        if (not left_attr_already_in) {
            left_required.emplace_back(left_entity, left_column);
        }
        if (not right_attr_already_in) {
            right_required.emplace_back(right_entity, right_column);
        }
        std::tie(left, left_columns) = recurse(*pleft, left_required);
        std::tie(right, right_columns) = recurse(*pright, right_required);
        size_t idx = 0;
        bool left_attr_set = false, right_attr_set = false;
        for (auto& [entity, column, _] : left_columns) {
            if (entity == left_entity and column == left_column) {
                left_attr = idx;
                left_attr_set = true;
                break;
            }
            ++idx;
        }
        idx = 0;
        for (auto& [entity, column, _] : right_columns) {
            if (entity == right_entity and column == right_column) {
                right_attr = idx;
                right_attr_set = true;
                break;
            }
            ++idx;
        }
        if (not left_attr_set or not right_attr_set) {
            throw std::runtime_error("Join conditions are not set properly");
        }
        left_columns.insert(left_columns.end(),
                            std::make_move_iterator(right_columns.begin()),
                            std::make_move_iterator(right_columns.end()));
        std::vector<std::tuple<TableEntity, std::string, DataType>> output_columns;
        std::vector<std::tuple<size_t, DataType>> output_attrs;
        for (const auto& [required_entity, required_column] : required_attrs) {
            bool found = false;
            size_t input_idx = 0;
            for (const auto& [entity, column, type] : left_columns) {
                if (entity == required_entity and column == required_column) {
                    output_columns.emplace_back(required_entity, required_column, type);
                    output_attrs.emplace_back(input_idx, type);
                    found = true;
                    break;
                }
                ++input_idx;
            }
            if (not found) {
                throw std::runtime_error(fmt::format(
                    "Cannot found the required attr: {}.{} in children's output",
                    required_entity,
                    required_column));
            }
        }
        auto new_node_id = ret.new_join_node(build_left,
                                             left,
                                             right,
                                             left_attr,
                                             right_attr,
                                             std::move(output_attrs));
        return {new_node_id, std::move(output_columns)};
    } else if (auto itr = scan_types.find(node_type); itr != scan_types.end()) {
        TableEntity entity;
        if (not node.contains("Alias")) {
            if (node.contains("Relation Name")) {
                auto relation_name = node["Relation Name"].get<std::string>();
                if (auto itr = table_counts.find(relation_name);
                    itr != table_counts.end()) {
                    if (itr->second == 1) {
                        entity.table = relation_name;
                        entity.id = 0;
                    } else {
                        throw std::runtime_error(
                            fmt::format("Table {} is used at least twice", relation_name));
                    }
                } else {
                    throw std::runtime_error(
                        fmt::format("Cannot find table: {}", relation_name));
                }
            } else {
                throw std::runtime_error(
                    "Neither \"Alias\" nor \"Relation Name\" exists in scan node");
            }
        } else {
            auto alias = node["Alias"].get<std::string>();
            if (auto itr = alias_map.find(alias); itr != alias_map.end()) {
                entity = itr->second;
            } else {
                throw std::runtime_error(fmt::format("Cannot find alias: {}", alias));
            }
        }
        const std::vector<Attribute>* pattributes;
        if (auto itr = ParsedSQL::attributes_map.find(entity.table); itr != ParsedSQL::attributes_map.end()) {
            pattributes = &itr->second;
        } else {
            throw std::runtime_error(
                fmt::format("Cannot find attributes for table: {}", entity.table));
        }
        Statement* filter = nullptr;
        if (auto itr = filters.find(entity); itr != filters.end()) {
            filter = itr->second.get();
        }
        std::string lookupName = fmt::format("{}|{}", entity.table, filter ? filter->pretty_print() : std::string{});

        lookupName = DataSource::Table::fixName(lookupName);
        auto it = db.tables.find(lookupName);
        if (it == db.tables.end()) {
            fmt::print("Table {} not found in cache, loading it\n", lookupName);
            auto table = Table::from_csv(*pattributes, fs::path("imdb") / fmt::format("{}.csv", entity.table), filter);
            db.columns.push_back(std::move(table));
            auto imported = PlanImport::importTable(db.columns.back());
            imported.name = lookupName;
            db.db.relations.push_back(std::move(imported));
            db.tables[lookupName] = db.db.relations.size() - 1;
        }
        auto new_input_id = db.tables.at(lookupName);

        std::vector<std::tuple<TableEntity, std::string, DataType>> output_columns;
        std::vector<std::tuple<size_t, DataType>> output_attrs;
        for (const auto& [required_entity, required_column] : required_attrs) {
            bool found = false;
            size_t input_idx = 0;
            for (const auto& attribute : *pattributes) {
                if (entity == required_entity and attribute.name == required_column) {
                    output_columns.emplace_back(required_entity,
                                                required_column,
                                                attribute.type);
                    output_attrs.emplace_back(input_idx, attribute.type);
                    found = true;
                    break;
                }
                ++input_idx;
            }
            if (not found) {
                throw std::runtime_error(fmt::format(
                    "Cannot found the required attr: {}.{} in children's output",
                    required_entity,
                    required_column));
            }
        }
        auto new_node_id = ret.new_scan_node(new_input_id, std::move(output_attrs));
        return {new_node_id, std::move(output_columns)};
    } else {
        throw std::runtime_error(fmt::format("Not supported node type: {}", node_type));
    }
}
//---------------------------------------------------------------------------
::Plan JoinPipelineLoader::load_join_pipeline(DataSourceBuilder& dbb, const json& node, const ParsedSQL& parsed_sql) {
    JoinPipelineLoader loader{parsed_sql, dbb};
    std::tie(loader.ret.root, std::ignore) = loader.recurse(node, parsed_sql.output_attrs);
    return std::move(loader.ret);
}
//---------------------------------------------------------------------------
}