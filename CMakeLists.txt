cmake_minimum_required(VERSION 3.16)

project(SigmodContest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Activate assertions in relwithdebinfo
option(FORCE_ASSERT "Force enable assertions in build" OFF)
if(FORCE_ASSERT)
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -UNDEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -UNDEBUG")
else()
    # Keep default behaviour
endif ()

# Activate assertions in relwithdebinfo
option(XRAY_ENABLE "Enable xray build" OFF)
if(XRAY_ENABLE)
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fxray-instrument")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fxray-instrument")
else()
    # Keep default behaviour
endif ()

Include(FetchContent)

FetchContent_Declare(
        Catch2
        URL https://github.com/catchorg/Catch2/archive/refs/tags/v3.8.0.tar.gz
)

FetchContent_MakeAvailable(Catch2)

find_library(re2
    NAMES re2
    PATHS /usr/lib /usr/local/lib
)

if(NOT re2)
    message(STATUS "re2 not found, acquiring...")
    FetchContent_Declare(
            abseil
            URL https://github.com/abseil/abseil-cpp/releases/download/20240722.1/abseil-cpp-20240722.1.tar.gz
    )

    set(ABSL_PROPAGATE_CXX_STD ON)
    set(ABSL_ENABLE_INSTALL ON)
    FetchContent_MakeAvailable(abseil)

    FetchContent_Declare(
            re2
            URL https://github.com/google/re2/releases/download/2024-07-02/re2-2024-07-02.tar.gz
    )

    FetchContent_MakeAvailable(re2)
else()
    message(STATUS "re2 found, using existing version")
endif()

FetchContent_Declare(
        json
        URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)

FetchContent_MakeAvailable(json)

FetchContent_Declare(
        sql-parser
        URL https://github.com/a858438680/sql-parser/archive/refs/tags/win-port-2.tar.gz
)
set(HSQL_ENABLE_WERROR OFF)
FetchContent_MakeAvailable(sql-parser)

FetchContent_Declare(
        range-v3
        URL https://github.com/ericniebler/range-v3/archive/refs/tags/0.12.0.tar.gz
)

FetchContent_MakeAvailable(range-v3)

FetchContent_Declare(
        fmtlib
        URL https://github.com/fmtlib/fmt/releases/download/11.1.3/fmt-11.1.3.zip
)

FetchContent_MakeAvailable(fmtlib)

FetchContent_Declare(
        duckdb
        URL https://github.com/duckdb/duckdb/archive/refs/tags/v1.2.1.tar.gz
)

option(ENABLE_SANITIZER "Enable Address and Undefined Behavior Sanitizer" OFF)
option(ENABLE_PERFETTO "Enable Perfetto tracing" OFF)

# If ENABLE_SANITIZER is set to ON, we need to set the sanitizer flags for the compiler.
if(ENABLE_SANITIZER)
    set(ENABLE_UBSAN ON)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,undefined -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined")
else()
    set(ENABLE_SANITIZER OFF)
    set(ENABLE_UBSAN OFF)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc|powerpc|ppc64|ppc64le")
    message("Disabling jemalloc extension of DuckDB on Power.")
    set(SKIP_EXTENSIONS jemalloc)
endif()
FetchContent_MakeAvailable(duckdb)

# Enable server-specific compiler optimizations.
# Use march=native for all but Power servers, which results in the following error:
#    clang++-18: error: unsupported option '-march=' for target 'powerpc64le-unknown-linux-gnu'
# This flag works on other Power systems, but for now, we disable march=native on all Power machines.
if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "ppc|powerpc|ppc64|ppc64le")
    add_compile_options(-march=native)
endif()

set(SIGMODPC_TEAMSRC
        # Our own sources
        engine/Execute.cpp
        engine/infra/Mmap.cpp
        engine/infra/Scheduler.cpp
        engine/infra/QueryMemory.cpp
        engine/infra/PageMemory.cpp
        engine/infra/JoinFilter.cpp
        engine/op/CollectorTarget.cpp
        engine/op/Hashtable.cpp
        engine/op/OpBase.cpp
        engine/op/ScanBase.cpp
        engine/op/TableScan.cpp
        engine/op/TableTarget.cpp
        engine/query/DataSource.cpp
        engine/query/PlanImport.cpp
        engine/query/Restriction.cpp
        engine/query/QueryGraph.cpp
        engine/query/QueryPlan.cpp
        engine/pipeline/JoinPipeline.cpp
        engine/pipeline/PipelineFunction.cpp
        engine/pipeline/PipelineGen.cpp
        engine/pipeline/PipelineGen0.cpp
        engine/pipeline/PipelineGen1.cpp
        engine/pipeline/PipelineGen2.cpp
        engine/pipeline/PipelineGen3.cpp
        engine/pipeline/PipelineGen4.cpp
        engine/pipeline/PipelineGen5.cpp
        engine/pipeline/PipelineGen6.cpp
        engine/pipeline/PipelineGen7.cpp
        engine/pipeline/PipelineGen8.cpp
        engine/pipeline/PipelineGen9.cpp
        engine/pipeline/PipelineGen10.cpp
        engine/pipeline/PipelineGen11.cpp
        engine/pipeline/PipelineGen12.cpp
        engine/pipeline/PipelineGen13.cpp
        engine/pipeline/PipelineGen14.cpp
        engine/pipeline/PipelineGen15.cpp
        engine/storage/BitLogic.cpp
        engine/storage/CopyLogic.cpp
        engine/storage/RestrictionLogic.cpp
)

set(SIGMODPC_SRC
        # Our own sources
        ${SIGMODPC_TEAMSRC}
        # DuckDB sources
        src/build_table.cpp
        src/csv_parser.cpp
        src/execute.cpp
        src/statement.cpp
)

add_executable(
        run

        ${SIGMODPC_SRC}
        tests/read_sql.cpp
)

target_include_directories(run PRIVATE include engine)
target_link_libraries(run PRIVATE re2 fmt range-v3 nlohmann_json::nlohmann_json sqlparser duckdb)
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_compile_definitions(run PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

add_executable(
        internal_runner

        ${SIGMODPC_SRC}
        engine/tools/DuckDB.cpp
        engine/tools/JoinPipelineLoader.cpp
        engine/tools/ParsedSQL.cpp
        engine/tools/Runner.cpp
        engine/tools/SQL.cpp
        engine/tools/Setting.cpp
)

target_include_directories(internal_runner PRIVATE include engine)
target_link_libraries(internal_runner PRIVATE re2 fmt range-v3 nlohmann_json::nlohmann_json sqlparser)

option(NO_DUCK "Build without duckdb" ON)
if(NO_DUCK)
    target_compile_definitions(internal_runner PRIVATE NO_DUCK)
else()
    target_link_libraries(internal_runner PRIVATE duckdb)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_compile_definitions(internal_runner PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

if(ENABLE_PERFETTO)
    target_compile_definitions(run PRIVATE PERFETTO)
    target_compile_definitions(internal_runner PRIVATE PERFETTO)
endif()

if(ENABLE_PERFEVENT)
    target_compile_definitions(internal_runner PRIVATE PERFEVENT)
endif()

target_compile_definitions(internal_runner PRIVATE SIGMOD_LOCAL)

add_executable(
        build_database

        tests/build_database.cpp
)

target_include_directories(build_database PRIVATE include)
target_link_libraries(build_database PRIVATE fmt duckdb)
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_compile_definitions(build_database PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

add_executable(
        unit_tests

        ${SIGMODPC_SRC}
        tests/unit_tests.cpp
        engine/test/unit_tests2.cpp
        engine/test/unit_tests3.cpp
)
target_compile_definitions(unit_tests PRIVATE SIGMOD_LOCAL)
target_compile_definitions(run PRIVATE SIGMOD_LOCAL)

target_include_directories(unit_tests PRIVATE include engine)
target_link_libraries(unit_tests PRIVATE range-v3 fmt Catch2::Catch2WithMain re2)
target_compile_definitions(unit_tests PRIVATE NO_DUCK)
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_compile_definitions(unit_tests PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()
target_compile_definitions(unit_tests PRIVATE SIGMOD_LOCAL)

message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "C++ RELEASE Flags: ${CMAKE_CXX_FLAGS_RELEASE}")
