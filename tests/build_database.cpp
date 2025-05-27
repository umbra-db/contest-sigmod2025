#include <common.h>

#include <duckdb.hpp>
#include <fmt/core.h>

int main(int argc, char* argv[]) {
    using namespace duckdb;
    namespace fs = std::filesystem;

    if (argc < 2) {
        fmt::println(stderr, "Usage: {} <DuckDB database file>", argv[0]);
        exit(EXIT_FAILURE);
    }

    auto schema = read_file(fs::path("job") / "schema.sql");

    DuckDB     db(argv[1]);
    Connection conn(db);
    auto       result = conn.Query(schema);
    if (result->HasError()) {
        fmt::println("Error: {}", result->GetError());
    }

    std::vector<std::string> table_names{
        "char_name",
        "kind_type",
        "cast_info",
        "movie_companies",
        "role_type",
        "complete_cast",
        "comp_cast_type",
        "company_name",
        "company_type",
        "movie_link",
        "movie_keyword",
        "name",
        "info_type",
        "movie_info_idx",
        "person_info",
        "link_type",
        "title",
        "aka_name",
        "movie_info",
        "keyword",
        "aka_title",
    };

    for (auto& table: table_names) {
        result =
            conn.Query(fmt::format("COPY {0} FROM 'imdb/{0}.csv' (ESCAPE '\\');", table));
        if (result->HasError()) {
            fmt::println("Error: {}", result->GetError());
        } else {
            fmt::println("Successfully loaded table {} into {}", table, argv[1]);
        }
    }
}
