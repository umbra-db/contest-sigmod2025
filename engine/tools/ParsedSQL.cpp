#include "tools/ParsedSQL.hpp"
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/range/conversion.hpp>
#include <SQLParser.h>
//---------------------------------------------------------------------------
template <>
struct fmt::formatter<hsql::ExprType> {
    template <class ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FormatContext>
    auto format(hsql::ExprType value, FormatContext& ctx) const {
        static std::array<std::string_view, 17> names{
            "kExprLiteralFloat",
            "kExprLiteralString",
            "kExprLiteralInt",
            "kExprLiteralNull",
            "kExprLiteralDate",
            "kExprLiteralInterval",
            "kExprStar",
            "kExprParameter",
            "kExprColumnRef",
            "kExprFunctionRef",
            "kExprOperator",
            "kExprSelect",
            "kExprHint",
            "kExprArray",
            "kExprArrayIndex",
            "kExprExtract",
            "kExprCast",
        };
        return fmt::format_to(ctx.out(), "{}", names[int(value)]);
    }
};
//---------------------------------------------------------------------------
template <>
struct fmt::formatter<hsql::OperatorType> {
    template <class ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FormatContext>
    auto format(hsql::OperatorType value, FormatContext& ctx) const {
        static std::array<std::string_view, 27> names{
            "kOpNone",

            // Ternary operator
            "kOpBetween",

            // n-nary special case
            "kOpCase",
            "kOpCaseListElement", // `WHEN expr THEN expr`

            // Binary operators.
            "kOpPlus",
            "kOpMinus",
            "kOpAsterisk",
            "kOpSlash",
            "kOpPercentage",
            "kOpCaret",

            "kOpEquals",
            "kOpNotEquals",
            "kOpLess",
            "kOpLessEq",
            "kOpGreater",
            "kOpGreaterEq",
            "kOpLike",
            "kOpNotLike",
            "kOpILike",
            "kOpAnd",
            "kOpOr",
            "kOpIn",
            "kOpConcat",

            // Unary operators.
            "kOpNot",
            "kOpUnaryMinus",
            "kOpIsNull",
            "kOpExists",
        };
        return fmt::format_to(ctx.out(), "{}", names[int(value)]);
    }
};
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
const std::unordered_map<std::string, std::vector<Attribute>> ParsedSQL::attributes_map = {
    {"aka_name",
     {{DataType::INT32, "id"},
      {DataType::INT32, "person_id"},
      {DataType::VARCHAR, "name"},
      {DataType::VARCHAR, "imdb_index"},
      {DataType::VARCHAR, "name_pcode_cf"},
      {DataType::VARCHAR, "name_pcode_nf"},
      {DataType::VARCHAR, "surname_pcode"},
      {DataType::VARCHAR, "md5sum"}}},
    {"aka_title",
     {{DataType::INT32, "id"},
      {DataType::INT32, "movie_id"},
      {DataType::VARCHAR, "title"},
      {DataType::VARCHAR, "imdb_index"},
      {DataType::INT32, "kind_id"},
      {DataType::INT32, "production_year"},
      {DataType::VARCHAR, "phonetic_code"},
      {DataType::INT32, "episode_of_id"},
      {DataType::INT32, "season_nr"},
      {DataType::INT32, "episode_nr"},
      {DataType::VARCHAR, "note"},
      {DataType::VARCHAR, "md5sum"}}},
    {"cast_info",
     {{DataType::INT32, "id"},
      {DataType::INT32, "person_id"},
      {DataType::INT32, "movie_id"},
      {DataType::INT32, "person_role_id"},
      {DataType::VARCHAR, "note"},
      {DataType::INT32, "nr_order"},
      {DataType::INT32, "role_id"}}},
    {"char_name",
     {{DataType::INT32, "id"},
      {DataType::VARCHAR, "name"},
      {DataType::VARCHAR, "imdb_index"},
      {DataType::INT32, "imdb_id"},
      {DataType::VARCHAR, "name_pcode_nf"},
      {DataType::VARCHAR, "surname_pcode"},
      {DataType::VARCHAR, "md5sum"}}},
    {"comp_cast_type", {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"company_name",
     {{DataType::INT32, "id"},
      {DataType::VARCHAR, "name"},
      {DataType::VARCHAR, "country_code"},
      {DataType::INT32, "imdb_id"},
      {DataType::VARCHAR, "name_pcode_nf"},
      {DataType::VARCHAR, "name_pcode_sf"},
      {DataType::VARCHAR, "md5sum"}}},
    {"company_type", {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"complete_cast",
     {{DataType::INT32, "id"},
      {DataType::INT32, "movie_id"},
      {DataType::INT32, "subject_id"},
      {DataType::INT32, "status_id"}}},
    {"info_type", {{DataType::INT32, "id"}, {DataType::VARCHAR, "info"}}},
    {"keyword",
     {{DataType::INT32, "id"},
      {DataType::VARCHAR, "keyword"},
      {DataType::VARCHAR, "phonetic_code"}}},
    {"kind_type", {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"link_type", {{DataType::INT32, "id"}, {DataType::VARCHAR, "link"}}},
    {"movie_companies",
     {{DataType::INT32, "id"},
      {DataType::INT32, "movie_id"},
      {DataType::INT32, "company_id"},
      {DataType::INT32, "company_type_id"},
      {DataType::VARCHAR, "note"}}},
    {"movie_info_idx",
     {{DataType::INT32, "id"},
      {DataType::INT32, "movie_id"},
      {DataType::INT32, "info_type_id"},
      {DataType::VARCHAR, "info"},
      {DataType::VARCHAR, "note"}}},
    {"movie_keyword",
     {{DataType::INT32, "id"},
      {DataType::INT32, "movie_id"},
      {DataType::INT32, "keyword_id"}}},
    {"movie_link",
     {{DataType::INT32, "id"},
      {DataType::INT32, "movie_id"},
      {DataType::INT32, "linked_movie_id"},
      {DataType::INT32, "link_type_id"}}},
    {"name",
     {{DataType::INT32, "id"},
      {DataType::VARCHAR, "name"},
      {DataType::VARCHAR, "imdb_index"},
      {DataType::INT32, "imdb_id"},
      {DataType::VARCHAR, "gender"},
      {DataType::VARCHAR, "name_pcode_cf"},
      {DataType::VARCHAR, "name_pcode_nf"},
      {DataType::VARCHAR, "surname_pcode"},
      {DataType::VARCHAR, "md5sum"}}},
    {"role_type", {{DataType::INT32, "id"}, {DataType::VARCHAR, "role"}}},
    {"title",
     {{DataType::INT32, "id"},
      {DataType::VARCHAR, "title"},
      {DataType::VARCHAR, "imdb_index"},
      {DataType::INT32, "kind_id"},
      {DataType::INT32, "production_year"},
      {DataType::INT32, "imdb_id"},
      {DataType::VARCHAR, "phonetic_code"},
      {DataType::INT32, "episode_of_id"},
      {DataType::INT32, "season_nr"},
      {DataType::INT32, "episode_nr"},
      {DataType::VARCHAR, "series_years"},
      {DataType::VARCHAR, "md5sum"}}},
    {"movie_info",
     {{DataType::INT32, "id"},
      {DataType::INT32, "movie_id"},
      {DataType::INT32, "info_type_id"},
      {DataType::VARCHAR, "info"},
      {DataType::VARCHAR, "note"}}},
    {"person_info",
     {{DataType::INT32, "id"},
      {DataType::INT32, "person_id"},
      {DataType::INT32, "info_type_id"},
      {DataType::VARCHAR, "info"},
      {DataType::VARCHAR, "note"}}}};
//---------------------------------------------------------------------------
void assert_column(hsql::Expr* expr) {
    if (expr->type != hsql::kExprColumnRef) {
        throw std::runtime_error(
            fmt::format("left side of \"Equals\" condition must be a ColumnRef"));
        exit(1);
    }
}
//---------------------------------------------------------------------------
void insert_filter(FilterMapType& filters, TableEntity entity, std::unique_ptr<Statement> stmt) {
    if (auto itr = filters.find(entity); itr == filters.end()) {
        filters.emplace(std::move(entity), std::move(stmt));
    } else {
        auto new_stmt = LogicalOperation::makeAnd(std::move(itr->second), std::move(stmt));
        itr->second = std::move(new_stmt);
    }
}
//---------------------------------------------------------------------------
size_t column_idx(const std::string& column, const TableEntity& entity) {
    auto& table = entity.table;
    if (auto itr = ParsedSQL::attributes_map.find(table); itr != ParsedSQL::attributes_map.end()) {
        auto& attributes = itr->second;
        for (size_t idx = 0; idx != attributes.size(); ++idx) {
            if (attributes[idx].name == column) {
                return idx;
            }
        }
        throw std::runtime_error(
            fmt::format("Cannot find column {} in table {}", column, table));
    } else {
        throw std::runtime_error(fmt::format("Cannot find table {}", table));
    }
}
//---------------------------------------------------------------------------
static std::tuple<std::string, TableEntity> extract_column_and_table(hsql::Expr* expr,
                                                                     const std::unordered_map<std::string, int>& table_counts,
                                                                     const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables,
                                                                     const AliasMapType& alias_map) {
    namespace views = ::ranges::views;
    using namespace std::string_literals;
    std::string column = expr->name;
    TableEntity table_entity;
    if (expr->hasTable()) {
        auto table = expr->table;
        if (auto itr = alias_map.find(table); itr != alias_map.end()) {
            table_entity = itr->second;
        } else if (auto itr = table_counts.find(table); itr != table_counts.end()) {
            auto count = itr->second;
            if (count == 1) {
                table_entity = {table, 0};
            } else {
                throw std::runtime_error(fmt::format("Ambiguous table: {}", table));
            }
        } else {
            throw std::runtime_error(fmt::format("Unknown table name: {}", table));
        }
    } else {
        if (auto itr = column_to_tables.find(column); itr != column_to_tables.end()) {
            if (itr->second.size() > 1) {
                throw std::runtime_error(
                    fmt::format("Ambiguous column: {0}, {1} have column {0}",
                                column,
                                itr->second | ::ranges::views::join(", "s) | ::ranges::to<std::string>()));
            } else {
                auto& table_name = itr->second[0];
                if (auto itr = table_counts.find(table_name); itr != table_counts.end()) {
                    auto count = itr->second;
                    if (count == 1) {
                        table_entity = {table_name, 0};
                    } else {
                        throw std::runtime_error(
                            fmt::format("Ambiguous table: {}", table_name));
                    }
                }
            }
        } else {
            throw std::runtime_error(fmt::format("No such column: {}", column));
        }
    }
    return {column, table_entity};
}
//---------------------------------------------------------------------------
void parse_expr_impl(hsql::Expr* expr,
                     const std::unordered_map<std::string, int>& table_counts,
                     const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables,
                     const AliasMapType& alias_map,
                     const ColumnMapType& column_map,
                     FilterMapType& filters,
                     DSU& join_union,
                     std::unique_ptr<Statement>& out_statement,
                     TableEntity& out_entity,
                     int level = 0) {
    switch (expr->type) {
        case hsql::kExprOperator: {
            auto op_type = expr->opType;
            switch (op_type) {
                case hsql::kOpAnd:
                case hsql::kOpOr: {
                    int add = 0;
                    if (op_type == hsql::kOpAnd) {
                        // fmt::println("operator And");
                    } else {
                        // fmt::println("operator Or");
                        add = 1;
                    }
                    std::unique_ptr<Statement> left;
                    TableEntity left_entity;
                    std::unique_ptr<Statement> right;
                    TableEntity right_entity;
                    // fmt::println("parse left");
                    parse_expr_impl(expr->expr,
                                    table_counts,
                                    column_to_tables,
                                    alias_map,
                                    column_map,
                                    filters,
                                    join_union,
                                    left,
                                    left_entity,
                                    level + add);
                    // fmt::println("parse right");
                    parse_expr_impl(expr->expr2,
                                    table_counts,
                                    column_to_tables,
                                    alias_map,
                                    column_map,
                                    filters,
                                    join_union,
                                    right,
                                    right_entity,
                                    level + add);
                    if (level == 0 and op_type == hsql::kOpAnd) {
                        // fmt::println("Top level And");
                        if (left) {
                            // fmt::println("insert left");
                            // left->pretty_print();
                            insert_filter(filters, std::move(left_entity), std::move(left));
                        }
                        if (right) {
                            // fmt::println("insert right: {}", (void*)right.get());
                            // right->pretty_print();
                            insert_filter(filters, std::move(right_entity), std::move(right));
                        }
                    } else {
                        if (!left or !right) {
                            throw std::runtime_error(
                                "Non top level contains join condition instead of filter");
                        }
                        if (left_entity != right_entity) {
                            throw std::runtime_error("Filter can not be pushed down");
                        }
                        if (op_type == hsql::kOpAnd) {
                            out_statement =
                                LogicalOperation::makeAnd(std::move(left), std::move(right));
                        } else {
                            out_statement = LogicalOperation::makeOr(std::move(left), std::move(right));
                        }
                        out_entity = left_entity;
                    }
                    break;
                }
                case hsql::kOpNot: {
                    // fmt::println("operator Not");
                    // fmt::println("parse child");
                    std::unique_ptr<Statement> child;
                    TableEntity child_entity;
                    parse_expr_impl(expr->expr,
                                    table_counts,
                                    column_to_tables,
                                    alias_map,
                                    column_map,
                                    filters,
                                    join_union,
                                    child,
                                    child_entity,
                                    level + 1);
                    out_statement = LogicalOperation::makeNot(std::move(child));
                    out_entity = std::move(child_entity);
                    break;
                }
                case hsql::kOpLess:
                case hsql::kOpLessEq:
                case hsql::kOpGreater:
                case hsql::kOpGreaterEq:
                case hsql::kOpEquals:
                case hsql::kOpNotEquals: {
                    Comparison::Op op;
                    // switch (op_type) {
                    // case hsql::kOpLess:      fmt::println("operator Less"); break;
                    // case hsql::kOpLessEq:    fmt::println("operator LessEq"); break;
                    // case hsql::kOpGreater:   fmt::println("operator Greater"); break;
                    // case hsql::kOpGreaterEq: fmt::println("operator GreaterEq"); break;
                    // case hsql::kOpEquals:    fmt::println("operator Equals"); break;
                    // case hsql::kOpNotEquals: fmt::println("operator Not Equals"); break;
                    // default:                 std::unreachable();
                    // }
                    switch (op_type) {
                        case hsql::kOpLess: op = Comparison::Op::LT; break;
                        case hsql::kOpLessEq: op = Comparison::Op::LEQ; break;
                        case hsql::kOpGreater: op = Comparison::Op::GT; break;
                        case hsql::kOpGreaterEq: op = Comparison::Op::GEQ; break;
                        case hsql::kOpEquals: op = Comparison::Op::EQ; break;
                        case hsql::kOpNotEquals: op = Comparison::Op::NEQ; break;
                        default: unreachable();
                    }
                    auto left = expr->expr;
                    auto right = expr->expr2;
                    assert_column(left);
                    auto [left_column, left_entity] =
                        extract_column_and_table(left, table_counts, column_to_tables, alias_map);
                    // fmt::println("left_column: {}", left_column);
                    // fmt::println("left_table: {}", left_table);
                    Literal value;
                    switch (right->type) {
                        case hsql::kExprLiteralInt: {
                            // fmt::println("int literal: {}", right->ival);
                            value = right->ival;
                            break;
                        }
                        case hsql::kExprLiteralString: {
                            // fmt::println("string literal: {}", right->name);
                            value = right->name;
                            break;
                        }
                        case hsql::kExprColumnRef: {
                            auto [right_column, right_entity] =
                                extract_column_and_table(right, table_counts, column_to_tables, alias_map);
                            if (op_type != hsql::kOpEquals) {
                                throw std::runtime_error("Non-EuqalJoins are not supported");
                            }
                            size_t left_column_idx, right_column_idx;
                            if (auto itr = column_map.find(left_entity); itr != column_map.end()) {
                                if (auto iter = itr->second.find(left_column); iter != itr->second.end()) {
                                    left_column_idx = iter->second;
                                } else {
                                    throw std::runtime_error(fmt::format("No column: {} in table: {}",
                                                                         left_column,
                                                                         left_entity));
                                }
                            } else {
                                throw std::runtime_error(fmt::format("No  table: {}", left_entity));
                            }
                            if (auto itr = column_map.find(right_entity); itr != column_map.end()) {
                                if (auto iter = itr->second.find(right_column); iter != itr->second.end()) {
                                    right_column_idx = iter->second;
                                } else {
                                    throw std::runtime_error(fmt::format("No column: {} in table: {}",
                                                                         right_column,
                                                                         right_entity));
                                }
                            } else {
                                throw std::runtime_error(fmt::format("No  table: {}", right_entity));
                            }
                            join_union.unite(left_column_idx, right_column_idx);
                            break;
                        }
                        default:
                            throw std::runtime_error(
                                fmt::format("Expression type: {} not processed", right->type));
                    }
                    if (right->type != hsql::kExprColumnRef) {
                        out_statement =
                            std::make_unique<Comparison>(column_idx(left_column, left_entity),
                                                         op,
                                                         std::move(value));
                        out_entity = std::move(left_entity);
                    }
                    break;
                }
                case hsql::kOpLike:
                case hsql::kOpNotLike: {
                    Comparison::Op op;
                    if (op_type == hsql::kOpLike) {
                        op = Comparison::Op::LIKE;
                        // fmt::println("operator Like");
                    } else {
                        op = Comparison::Op::NOT_LIKE;
                        // fmt::println("operator Not Like");
                    }
                    auto left = expr->expr;
                    auto right = expr->expr2;
                    assert_column(left);
                    auto [left_column, left_entity] =
                        extract_column_and_table(left, table_counts, column_to_tables, alias_map);
                    // fmt::println("left_column: {}", left_column);
                    // fmt::println("left_table: {}", left_table);
                    switch (right->type) {
                        case hsql::kExprLiteralString: {
                            // fmt::println("string literal: {}", right->name);
                            Literal value = right->name;
                            out_statement =
                                std::make_unique<Comparison>(column_idx(left_column, left_entity),
                                                             op,
                                                             std::move(value));
                            out_entity = std::move(left_entity);
                            break;
                        }
                        default:
                            throw std::runtime_error(
                                fmt::format("Expression type: {} not processed", right->type));
                    }
                    break;
                }
                case hsql::kOpBetween: {
                    // fmt::println("operator Between");
                    auto left = expr->expr;
                    auto& list = *expr->exprList;
                    assert_column(left);
                    auto [left_column, left_entity] =
                        extract_column_and_table(left, table_counts, column_to_tables, alias_map);
                    // fmt::println("left_column: {}", left_column);
                    // fmt::println("left_table: {}", left_table);
                    Literal values[2];
                    for (size_t idx = 0; idx != list.size(); ++idx) {
                        auto& item = list[idx];
                        switch (item->type) {
                            case hsql::kExprLiteralInt: {
                                // fmt::println("item {}: int literal: {}", idx, item->ival);
                                values[idx] = item->ival;
                                break;
                            }
                            case hsql::kExprLiteralString: {
                                // fmt::println("item {}: string literal: {}", idx, item->name);
                                values[idx] = item->name;
                                break;
                            }
                            default:
                                throw std::runtime_error(
                                    fmt::format("Expression type: {} not processed", item->type));
                        }
                    }
                    auto stmt1 = std::make_unique<Comparison>(column_idx(left_column, left_entity),
                                                              Comparison::Op::GEQ,
                                                              std::move(values[0]));
                    auto stmt2 = std::make_unique<Comparison>(column_idx(left_column, left_entity),
                                                              Comparison::Op::LEQ,
                                                              std::move(values[1]));
                    out_statement = LogicalOperation::makeAnd(std::move(stmt1), std::move(stmt2));
                    out_entity = std::move(left_entity);
                    break;
                }
                case hsql::kOpIn: {
                    // fmt::println("operator In");
                    auto left = expr->expr;
                    auto& list = *expr->exprList;
                    assert_column(left);
                    auto [left_column, left_entity] =
                        extract_column_and_table(left, table_counts, column_to_tables, alias_map);
                    // fmt::println("left_column: {}", left_column);
                    // fmt::println("left_table: {}", left_table);
                    for (size_t idx = 0; idx != list.size(); ++idx) {
                        auto& item  = list[idx];
                        Literal value;
                        switch (item->type) {
                            case hsql::kExprLiteralInt: {
                                // fmt::println("item {}: int literal: {}", idx, item->ival);
                                value = item->ival;
                                break;
                            }
                            case hsql::kExprLiteralString: {
                                // fmt::println("item {}: string literal: {}", idx, item->name);
                                value = item->name;
                                break;
                            }
                            default:
                                throw std::runtime_error(
                                    fmt::format("Expression type: {} not processed", item->type));
                        }
                        if (not out_statement) {
                            out_statement =
                                std::make_unique<Comparison>(column_idx(left_column, left_entity),
                                                             Comparison::Op::EQ,
                                                             std::move(value));
                        } else {
                            auto new_stmt =
                                std::make_unique<Comparison>(column_idx(left_column, left_entity),
                                                             Comparison::Op::EQ,
                                                             std::move(value));
                            out_statement =
                                LogicalOperation::makeOr(std::move(out_statement), std::move(new_stmt));
                        }
                    }
                    out_entity = std::move(left_entity);
                    break;
                }
                case hsql::kOpIsNull: {
                    // fmt::println("operator IsNull");
                    auto child = expr->expr;
                    assert_column(child);
                    auto [child_column, child_entity] =
                        extract_column_and_table(child, table_counts, column_to_tables, alias_map);
                    // fmt::println("child_column: {}", child_column);
                    // fmt::println("child_table: {}", child_table);
                    out_statement = std::make_unique<Comparison>(column_idx(child_column, child_entity),
                                                                 Comparison::Op::IS_NULL,
                                                                 std::monostate{});
                    out_entity = std::move(child_entity);
                    break;
                }
                default: {
                    throw std::runtime_error(fmt::format("Operator type: {} not processed", op_type));
                }
            }
            break;
        }
        default: {
            throw std::runtime_error(fmt::format("Expression type: {} not processed", expr->type));
        }
    }
}
//---------------------------------------------------------------------------
void parse_expr(hsql::Expr* expr,
                const std::unordered_map<std::string, int>& table_counts,
                const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables,
                const AliasMapType& alias_map,
                const ColumnMapType& column_map,
                FilterMapType& filters,
                DSU& join_union) {
    std::unique_ptr<Statement> top_statement;
    TableEntity top_entity;
    parse_expr_impl(expr,
                    table_counts,
                    column_to_tables,
                    alias_map,
                    column_map,
                    filters,
                    join_union,
                    top_statement,
                    top_entity);
    if (top_statement) {
        insert_filter(filters, std::move(top_entity), std::move(top_statement));
    }
}
//---------------------------------------------------------------------------
ParsedSQL::ParsedSQL(const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables)
    : column_to_tables(column_to_tables) {}
//---------------------------------------------------------------------------
std::string ParsedSQL::executed_sql(const std::string& sql) {
    namespace views = ::ranges::views;
    using namespace std::string_literals;

    auto select_list =
        output_attrs | views::transform([this](const std::tuple<TableEntity, std::string>& attr) {
            auto& [entity, column] = attr;
            if (auto itr = entity_to_alias.find(entity); itr != entity_to_alias.end()) {
                return fmt::format("{}.{}", itr->second, column);
            } else {
                return fmt::format("{}.{}", entity.table, column);
            }
        }) |
        ::ranges::views::join(", "s) | ::ranges::to<std::string>();

    auto from_clause_begin = sql.find("FROM");
    if (from_clause_begin == std::string::npos) {
        from_clause_begin = sql.find("from");
        if (from_clause_begin == std::string::npos) {
            throw std::runtime_error("Cannot find \"FROM\" or \"from\" in sql");
        }
    }
    size_t num_trailing_space = 0;
    for (auto itr = sql.rbegin(); itr != sql.rend(); ++itr) {
        if (*itr != ';') {
            ++num_trailing_space;
        } else {
            break;
        }
    }
    std::string_view other = std::string_view{sql.data() + from_clause_begin,
                                              sql.size() - num_trailing_space - from_clause_begin};

    return fmt::format("SELECT {} {}", select_list, other);
}
//---------------------------------------------------------------------------
void ParsedSQL::parse_sql(const std::string& sql, std::string_view name) {
    hsql::SQLParserResult sql_result;
    hsql::SQLParser::parse(sql, &sql_result);

    if (not sql_result.isValid()) {
        throw std::runtime_error(fmt::format("Error parsing SQL: {}", name));
    }
    auto statement = (const hsql::SelectStatement*) sql_result.getStatement(0);

    size_t column_count = 0;
    auto handleTable = [&](hsql::TableRef* table) {
        if (table->type != hsql::kTableName) {
            throw std::runtime_error("SQL not supported");
        }
        auto table_itr = table_counts.find(table->name);
        if (table_itr == table_counts.end()) {
            bool _;
            std::tie(table_itr, _) = table_counts.emplace(table->name, 1);
        } else {
            ++(table_itr->second);
        }
        TableEntity entity{table->name, table_itr->second - 1};
        auto [itr, _] =
            column_map.emplace(entity, std::unordered_map<std::string, size_t>{});
        if (auto attr_itr = attributes_map.find(entity.table);
            attr_itr != attributes_map.end()) {
            for (auto& attr : attr_itr->second) {
                itr->second.emplace(attr.name, column_count++);
                column_vec.emplace_back(entity, attr.name);
            }
        } else {
            throw std::runtime_error(fmt::format("No table: {} in schema", entity.table));
        }
        auto alias = table->alias;
        if (alias) {
            alias_map.emplace(alias->name, entity);
            entity_to_alias.emplace(entity, alias->name);
        }
    };

    auto fromTable = statement->fromTable;
    if (fromTable->type == hsql::kTableName) {
        handleTable(fromTable);
    } else if (fromTable->type == hsql::kTableCrossProduct) {
        auto fromTableList = fromTable->list;
        for (auto table : *fromTableList) {
            handleTable(table);
        }
    } else {
        throw std::runtime_error("SQL not supported");
    }

    auto& selectList = *statement->selectList;
    for (auto* expr : selectList) {
        switch (expr->type) {
            case (hsql::kExprFunctionRef): {
                for (auto* child : *expr->exprList) {
                    // fmt::println("Child: {}", child->type);
                    if (child->type != hsql::kExprColumnRef) {
                        throw std::runtime_error(
                            "Complex select expressions are not supported");
                    }
                    auto [column, entity] = extract_column_and_table(child,
                                                                     table_counts,
                                                                     column_to_tables,
                                                                     alias_map);
                    output_attrs.emplace_back(entity, column);
                }
                break;
            }
            case (hsql::kExprColumnRef): {
                auto [column, entity] =
                    extract_column_and_table(expr, table_counts, column_to_tables, alias_map);
                output_attrs.emplace_back(entity, column);
                break;
            }
            default: {
                throw std::runtime_error(
                    fmt::format("Not supported expression type in select list: {}.",
                                expr->type));
                break;
            }
        }
    }
    auto condition = statement->whereClause;
    DSU join_union(column_count);
    parse_expr(condition,
               table_counts,
               column_to_tables,
               alias_map,
               column_map,
               filters,
               join_union);

    std::unordered_map<size_t, std::vector<size_t>> joined_columns;
    for (size_t i = 0; i < column_count; ++i) {
        auto set = join_union.find(i);
        if (auto itr = joined_columns.find(set); itr != joined_columns.end()) {
            itr->second.emplace_back(i);
        } else {
            joined_columns.emplace(set, std::vector<size_t>(1, i));
        }
    }

    for (auto& [set, items] : joined_columns) {
        for (size_t i = 0; i < items.size() - 1; ++i) {
            for (auto j = i + 1; j < items.size(); ++j) {
                auto& [left_entity, left_column] = column_vec[items[i]];
                auto& [right_entity, right_column] = column_vec[items[j]];
                if (auto itr = join_graph.find(left_entity); itr != join_graph.end()) {
                    if (auto iter = itr->second.find(right_entity);
                        iter != itr->second.end()) {
                        throw std::runtime_error(
                            "At least two conditions between a same pair of tables.");
                    }
                    itr->second.emplace(right_entity,
                                        std::tuple{left_column, right_column});
                } else {
                    std::unordered_map<TableEntity, std::tuple<std::string, std::string>>
                        adj_item;
                    adj_item.emplace(right_entity, std::tuple{left_column, right_column});
                    join_graph.emplace(left_entity, std::move(adj_item));
                }
                if (auto itr = join_graph.find(right_entity); itr != join_graph.end()) {
                    itr->second.emplace(left_entity, std::tuple{right_column, left_column});
                } else {
                    std::unordered_map<TableEntity, std::tuple<std::string, std::string>>
                        adj_item;
                    adj_item.emplace(left_entity, std::tuple{right_column, left_column});
                    join_graph.emplace(right_entity, std::move(adj_item));
                }
            }
        }
    }
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------