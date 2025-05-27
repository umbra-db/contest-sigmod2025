#include <catch2/catch_test_macros.hpp>

#include <table.h>
#include <plan.h>

void sort(std::vector<std::vector<Data>>& table) {
    std::sort(table.begin(), table.end());
}

namespace Catch {
template<>
struct StringMaker<Data> {
   static std::string convert( Data const& value ) {
      return fmt::format("{}", value);
   }
};
}

TEST_CASE("Empty join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    ColumnarTable table1, table2;
    table1.columns.emplace_back(DataType::INT32);
    table2.columns.emplace_back(DataType::INT32);
    plan.inputs.emplace_back(std::move(table1));
    plan.inputs.emplace_back(std::move(table2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 0);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
}

TEST_CASE("One line join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data{
        {1, },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table(std::move(data), std::move(types));
    ColumnarTable table1 = table.to_columnar();
    ColumnarTable table2 = table.to_columnar();
    plan.inputs.emplace_back(std::move(table1));
    plan.inputs.emplace_back(std::move(table2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 1);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, 1,},
    };
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Simple join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data{
        {1,},
        {2,},
        {3,},
    };
    std::vector<DataType> types{DataType::INT32};
    Table table(std::move(data), std::move(types));
    ColumnarTable table1 = table.to_columnar();
    ColumnarTable table2 = table.to_columnar();
    plan.inputs.emplace_back(std::move(table1));
    plan.inputs.emplace_back(std::move(table2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 3);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, 1,},
        {2, 2,},
        {3, 3,},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Empty Result", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data1{
        {1,},
        {2,},
        {3,},
    };
    std::vector<std::vector<Data>> data2{
        {4,},
        {5,},
        {6,},
    };
    std::vector<DataType> types{DataType::INT32};
    Table table1(std::move(data1), types);
    Table table2(std::move(data2), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table2.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 0);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
}

TEST_CASE("Multiple same keys", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data1{
        {1,},
        {1,},
        {2,},
        {3,},
    };
    std::vector<DataType> types{DataType::INT32};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {2, 2,},
        {3, 3,},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("NULL keys", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data1{
        {1,               },
        {1,               },
        {std::monostate{},},
        {2,               },
        {3,               },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {2, 2,},
        {3, 3,},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Multiple columns", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{1, DataType::VARCHAR}, {0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 1, {{0, DataType::INT32}, {2, DataType::INT32}, {1, DataType::VARCHAR}});
    using namespace std::string_literals;
    std::vector<std::vector<Data>> data1{
        {1               , "xxx"s,},
        {1               , "yyy"s,},
        {std::monostate{}, "zzz"s,},
        {2               , "uuu"s,},
        {3               , "vvv"s,},
    };
    std::vector<DataType> types{DataType::INT32, DataType::VARCHAR};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 3);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    REQUIRE(result.columns[2].type == DataType::VARCHAR);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, 1, "xxx"s},
        {1, 1, "xxx"s},
        {1, 1, "yyy"s},
        {1, 1, "yyy"s},
        {2, 2, "uuu"s},
        {3, 3, "vvv"s},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Build on right", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{1, DataType::VARCHAR}, {0, DataType::INT32}});
    plan.new_join_node(false, 0, 1, 0, 1, {{0, DataType::INT32}, {2, DataType::INT32}, {1, DataType::VARCHAR}});
    using namespace std::string_literals;
    std::vector<std::vector<Data>> data1{
        {10               , "xxx"s,},
        {10               , "yyy"s,},
        {std::monostate{}, "zzz"s,},
        {20               , "uuu"s,},
        {30               , "vvv"s,},
    };
    std::vector<DataType> types{DataType::INT32, DataType::VARCHAR};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 3);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    REQUIRE(result.columns[2].type == DataType::VARCHAR);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {10, 10, "xxx"s},
        {10, 10, "xxx"s},
        {10, 10, "yyy"s},
        {10, 10, "yyy"s},
        {20, 20, "uuu"s},
        {30, 30, "vvv"s},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}


TEST_CASE("Ternary", "[join]") {
   Plan plan;
   plan.new_scan_node(0, {{0, DataType::INT32}});
   plan.new_scan_node(1, {{1, DataType::VARCHAR}, {0, DataType::INT32}});
   plan.new_scan_node(2, {{0, DataType::INT32}});
   plan.new_join_node(false, 0, 1, 0, 1, {{0, DataType::INT32}, {2, DataType::INT32}, {1, DataType::VARCHAR}});
   plan.new_join_node(false, 2, 3, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}, {3, DataType::VARCHAR}});
   using namespace std::string_literals;
   std::vector<std::vector<Data>> data1{
      {10               , "xxx"s,},
      {10               , "yyy"s,},
      {std::monostate{}, "zzz"s,},
      {20               , "uuu"s,},
      {30               , "vvv"s,},
   };
   std::vector<DataType> types{DataType::INT32, DataType::VARCHAR};
   Table table1(std::move(data1), std::move(types));
   ColumnarTable input1 = table1.to_columnar();
   ColumnarTable input2 = table1.to_columnar();
   ColumnarTable input3 = table1.to_columnar();
   plan.inputs.emplace_back(std::move(input1));
   plan.inputs.emplace_back(std::move(input2));
   plan.inputs.emplace_back(std::move(input3));
   plan.root = 4;
   auto* context = Contest::build_context();
   auto result = Contest::execute(plan, context);
   Contest::destroy_context(context);
   REQUIRE(result.num_rows == 10);
   REQUIRE(result.columns.size() == 3);
   REQUIRE(result.columns[0].type == DataType::INT32);
   REQUIRE(result.columns[1].type == DataType::INT32);
   REQUIRE(result.columns[2].type == DataType::VARCHAR);
   auto result_table = Table::from_columnar(result);
   std::vector<std::vector<Data>> ground_truth{
      {10, 10, "xxx"s},
      {10, 10, "xxx"s},
      {10, 10, "xxx"s},
      {10, 10, "xxx"s},
      {10, 10, "yyy"s},
      {10, 10, "yyy"s},
      {10, 10, "yyy"s},
      {10, 10, "yyy"s},
      {20, 20, "uuu"s},
      {30, 30, "vvv"s},
   };
   sort(result_table.table());
   REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("CrossProduct", "[join]") {
   Plan plan;
   plan.new_scan_node(0, {{0, DataType::INT32}, {1, DataType::VARCHAR}});
   plan.new_scan_node(1, {{0, DataType::INT32}, {1, DataType::INT32}});
   plan.new_scan_node(2, {{0, DataType::INT32}, {1, DataType::VARCHAR}});
   plan.new_join_node(false, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::VARCHAR}, {2, DataType::INT32}, {3, DataType::INT32}, });
   plan.new_join_node(false, 3, 2, 3, 0, {{0, DataType::INT32}, {1, DataType::VARCHAR}, {2, DataType::INT32}, {3, DataType::INT32}, {4, DataType::INT32}, {5, DataType::VARCHAR}, });
   using namespace std::string_literals;
   std::vector<std::vector<Data>> data1{
      {10               , "xxx"s,},
      {10               , "xxx"s,},
      {10               , "yyy"s,},
      {std::monostate{}, "zzz"s,},
      {20               , "uuu"s,},
      {20               , "uuu"s,},
      {30               , "vvv"s,},
   };
   std::vector<std::vector<Data>> data2{
      {10               , 20,},
   };
   std::vector<DataType> types{DataType::INT32, DataType::VARCHAR};
   std::vector<DataType> types2{DataType::INT32, DataType::INT32};
   Table table1(std::move(data1), types);
   Table table2(std::move(data2), types2);
   ColumnarTable input1 = table1.to_columnar();
   ColumnarTable input2 = table2.to_columnar();
   ColumnarTable input3 = table1.to_columnar();
   plan.inputs.emplace_back(std::move(input1));
   plan.inputs.emplace_back(std::move(input2));
   plan.inputs.emplace_back(std::move(input3));
   plan.root = 4;
   auto* context = Contest::build_context();
   auto result = Contest::execute(plan, context);
   Contest::destroy_context(context);
   REQUIRE(result.num_rows == 6);
   REQUIRE(result.columns.size() == 6);
   REQUIRE(result.columns[0].type == DataType::INT32);
   REQUIRE(result.columns[1].type == DataType::VARCHAR);
   REQUIRE(result.columns[2].type == DataType::INT32);
   REQUIRE(result.columns[3].type == DataType::INT32);
   REQUIRE(result.columns[4].type == DataType::INT32);
   REQUIRE(result.columns[5].type == DataType::VARCHAR);
   auto result_table = Table::from_columnar(result);
   std::vector<std::vector<Data>> ground_truth{
      {10, "xxx"s, 10, 20, 20, "uuu"s},
      {10, "xxx"s, 10, 20, 20, "uuu"s},
      {10, "xxx"s, 10, 20, 20, "uuu"s},
      {10, "xxx"s, 10, 20, 20, "uuu"s},
      {10, "yyy"s, 10, 20, 20, "uuu"s},
      {10, "yyy"s, 10, 20, 20, "uuu"s},
   };
   sort(result_table.table());
   REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Extended CrossProduct 5 Tables", "[join]") {
   Plan plan;
   // Scan nodes for:
   // R(a, b): non-singleton (a: INT32, b: VARCHAR)
   plan.new_scan_node(0, {{0, DataType::INT32}, {1, DataType::VARCHAR}});
   // S(b, c): singleton (both INT32)
   plan.new_scan_node(1, {{0, DataType::INT32}, {1, DataType::INT32}});
   // T(c, d, e): non-singleton (c: INT32, d: VARCHAR, e: INT32)
   plan.new_scan_node(2, {{0, DataType::INT32}, {1, DataType::VARCHAR}, {2, DataType::INT32}});
   // U(e, f): singleton (both INT32)
   plan.new_scan_node(3, {{0, DataType::INT32}, {1, DataType::INT32}});
   // V(f, g): non-singleton (f: INT32, g: VARCHAR)
   plan.new_scan_node(4, {{0, DataType::INT32}, {1, DataType::VARCHAR}});

   // Join R and S on R.col0 == S.col0.
   // Output schema: {R.a, R.b, S.b, S.c}
   plan.new_join_node(false, 0, 1, 0, 0, {
                                            {0, DataType::INT32}, {1, DataType::VARCHAR},
                                            {2, DataType::INT32}, {3, DataType::INT32}
                                         });
   // Join result with T on (left.col3 == T.col0).
   // Output schema becomes: {R.a, R.b, S.b, S.c, T.c, T.d, T.e}
   plan.new_join_node(false, 5, 2, 3, 0, {
                                            {0, DataType::INT32}, {1, DataType::VARCHAR},
                                            {2, DataType::INT32}, {3, DataType::INT32},
                                            {4, DataType::INT32}, {5, DataType::VARCHAR}, {6, DataType::INT32}
                                         });
   // Join with U on (left.col6 == U.col0).
   // New schema: previous columns appended with U’s two columns.
   plan.new_join_node(false, 6, 3, 6, 0, {
                                            {0, DataType::INT32}, {1, DataType::VARCHAR},
                                            {2, DataType::INT32}, {3, DataType::INT32},
                                            {4, DataType::INT32}, {5, DataType::VARCHAR}, {6, DataType::INT32},
                                            {7, DataType::INT32}, {8, DataType::INT32}
                                         });
   // Finally join with V on (left.col8 == V.col0).
   // Final schema: 11 columns.
   plan.new_join_node(false, 7, 4, 8, 0, {
                                            {0, DataType::INT32}, {1, DataType::VARCHAR},
                                            {2, DataType::INT32}, {3, DataType::INT32},
                                            {4, DataType::INT32}, {5, DataType::VARCHAR}, {6, DataType::INT32},
                                            {7, DataType::INT32}, {8, DataType::INT32},
                                            {9, DataType::INT32}, {10, DataType::VARCHAR}
                                         });

   // Prepare test data.
   using namespace std::string_literals;
   // Table R (non-singleton):
   std::vector<std::vector<Data>> dataR{
      {10, "r1"s},
      {20, "r2"s},
      {30, "r3"s}
   };
   // Table S (singleton):
   std::vector<std::vector<Data>> dataS{
      {10, 100}
   };
   // Table T (non-singleton; only rows with T.col0 == 100 join; T.e will be used later)
   std::vector<std::vector<Data>> dataT{
      {100, "200"s, 300},
      {100, "210"s, 300},
      {101, "220"s, 300}  // non-matching row
   };
   // Table U (singleton):
   std::vector<std::vector<Data>> dataU{
      {300, 400}
   };
   // Table V (non-singleton; only rows with V.col0 == 400 join)
   std::vector<std::vector<Data>> dataV{
      {400, "v1"s},
      {401, "v2"s}  // non-matching row
   };

   std::vector<DataType> typesR{DataType::INT32, DataType::VARCHAR};
   std::vector<DataType> typesS{DataType::INT32, DataType::INT32};
   std::vector<DataType> typesT{DataType::INT32, DataType::VARCHAR, DataType::INT32};
   std::vector<DataType> typesU{DataType::INT32, DataType::INT32};
   std::vector<DataType> typesV{DataType::INT32, DataType::VARCHAR};

   Table tableR(std::move(dataR), typesR);
   Table tableS(std::move(dataS), typesS);
   Table tableT(std::move(dataT), typesT);
   Table tableU(std::move(dataU), typesU);
   Table tableV(std::move(dataV), typesV);

   ColumnarTable inputR = tableR.to_columnar();
   ColumnarTable inputS = tableS.to_columnar();
   ColumnarTable inputT = tableT.to_columnar();
   ColumnarTable inputU = tableU.to_columnar();
   ColumnarTable inputV = tableV.to_columnar();

   plan.inputs.emplace_back(std::move(inputR));
   plan.inputs.emplace_back(std::move(inputS));
   plan.inputs.emplace_back(std::move(inputT));
   plan.inputs.emplace_back(std::move(inputU));
   plan.inputs.emplace_back(std::move(inputV));

   plan.root = 8; // final join node id

   auto* context = Contest::build_context();
   auto result = Contest::execute(plan, context);
   Contest::destroy_context(context);

   // Expected final result (2 rows):
   // Row 1: [10, "r1", 10, 100, 100, "200", 300, 300, 400, 400, "v1"]
   // Row 2: [10, "r1", 10, 100, 100, "210", 300, 300, 400, 400, "v1"]
   auto result_table = Table::from_columnar(result);
   std::vector<std::vector<Data>> ground_truth{
      {10, "r1"s, 10, 100, 100, "200"s, 300, 300, 400, 400, "v1"s},
      {10, "r1"s, 10, 100, 100, "210"s, 300, 300, 400, 400, "v1"s}
   };
   sort(result_table.table());
   REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Extended CrossProduct 7 Tables", "[join]") {
   Plan plan;
   // For the 7-table join we use simple INT32 schemas.
   // Table0: non-singleton.
   plan.new_scan_node(0, {{0, DataType::INT32}, {1, DataType::INT32}});
   // Table1: singleton.
   plan.new_scan_node(1, {{0, DataType::INT32}, {1, DataType::INT32}});
   // Table2: non-singleton.
   plan.new_scan_node(2, {{0, DataType::INT32}, {1, DataType::INT32}});
   // Table3: singleton.
   plan.new_scan_node(3, {{0, DataType::INT32}, {1, DataType::INT32}});
   // Table4: non-singleton.
   plan.new_scan_node(4, {{0, DataType::INT32}, {1, DataType::INT32}});
   // Table5: singleton.
   plan.new_scan_node(5, {{0, DataType::INT32}, {1, DataType::INT32}});
   // Table6: non-singleton.
   plan.new_scan_node(6, {{0, DataType::INT32}, {1, DataType::INT32}});

   // Build the join chain.
   // Join node 1: join Table0 and Table1 on Table0.col0 == Table1.col0.
   plan.new_join_node(false, 0, 1, 0, 0, {
                                            {0, DataType::INT32}, {1, DataType::INT32},
                                            {2, DataType::INT32}, {3, DataType::INT32}
                                         });
   // Join node 2: join result (id=7) with Table2 on result.col3 == Table2.col0.
   plan.new_join_node(false, 7, 2, 3, 0, {
                                            {0, DataType::INT32}, {1, DataType::INT32},
                                            {2, DataType::INT32}, {3, DataType::INT32},
                                            {4, DataType::INT32}, {5, DataType::INT32}
                                         });
   // Join node 3: join result (id=8) with Table3 on result.col5 == Table3.col0.
   plan.new_join_node(false, 8, 3, 5, 0, {
                                            {0, DataType::INT32}, {1, DataType::INT32},
                                            {2, DataType::INT32}, {3, DataType::INT32},
                                            {4, DataType::INT32}, {5, DataType::INT32},
                                            {6, DataType::INT32}, {7, DataType::INT32}
                                         });
   // Join node 4: join result (id=9) with Table4 on result.col7 == Table4.col0.
   plan.new_join_node(false, 9, 4, 7, 0, {
                                            {0, DataType::INT32}, {1, DataType::INT32},
                                            {2, DataType::INT32}, {3, DataType::INT32},
                                            {4, DataType::INT32}, {5, DataType::INT32},
                                            {6, DataType::INT32}, {7, DataType::INT32},
                                            {8, DataType::INT32}, {9, DataType::INT32}
                                         });
   // Join node 5: join result (id=10) with Table5 on result.col9 == Table5.col0.
   plan.new_join_node(false, 10, 5, 9, 0, {
                                             {0, DataType::INT32}, {1, DataType::INT32},
                                             {2, DataType::INT32}, {3, DataType::INT32},
                                             {4, DataType::INT32}, {5, DataType::INT32},
                                             {6, DataType::INT32}, {7, DataType::INT32},
                                             {8, DataType::INT32}, {9, DataType::INT32},
                                             {10, DataType::INT32}, {11, DataType::INT32}
                                          });
   // Join node 6: join result (id=11) with Table6 on result.col11 == Table6.col0.
   plan.new_join_node(false, 11, 6, 11, 0, {
                                              {0, DataType::INT32},  {1, DataType::INT32},
                                              {2, DataType::INT32},  {3, DataType::INT32},
                                              {4, DataType::INT32},  {5, DataType::INT32},
                                              {6, DataType::INT32},  {7, DataType::INT32},
                                              {8, DataType::INT32},  {9, DataType::INT32},
                                              {10, DataType::INT32}, {11, DataType::INT32},
                                              {12, DataType::INT32}, {13, DataType::INT32}
                                           });

   // Prepare test data.
   // Table0 (non-singleton): only the row with col0 == 10 will join.
   std::vector<std::vector<Data>> data0{
      {10, 111},
      {20, 222}
   };
   // Table1 (singleton): must have col0 == 10.
   std::vector<std::vector<Data>> data1{
      {10, 100}
   };
   // Table2 (non-singleton): require col0 == 100.
   std::vector<std::vector<Data>> data2{
      {100, 200},
      {100, 210}
   };
   // Table3 (singleton): must have col0 == 200.
   std::vector<std::vector<Data>> data3{
      {200, 300}
   };
   // Table4 (non-singleton): require col0 == 300.
   std::vector<std::vector<Data>> data4{
      {300, 400},
      {300, 410}
   };
   // Table5 (singleton): must have col0 == 400.
   std::vector<std::vector<Data>> data5{
      {400, 500}
   };
   // Table6 (non-singleton): require col0 == 500.
   std::vector<std::vector<Data>> data6{
      {500, 600},
      {500, 610}
   };

   std::vector<DataType> types0{DataType::INT32, DataType::INT32};
   std::vector<DataType> types1{DataType::INT32, DataType::INT32};
   std::vector<DataType> types2{DataType::INT32, DataType::INT32};
   std::vector<DataType> types3{DataType::INT32, DataType::INT32};
   std::vector<DataType> types4{DataType::INT32, DataType::INT32};
   std::vector<DataType> types5{DataType::INT32, DataType::INT32};
   std::vector<DataType> types6{DataType::INT32, DataType::INT32};

   Table table0(std::move(data0), types0);
   Table table1(std::move(data1), types1);
   Table table2(std::move(data2), types2);
   Table table3(std::move(data3), types3);
   Table table4(std::move(data4), types4);
   Table table5(std::move(data5), types5);
   Table table6(std::move(data6), types6);

   ColumnarTable input0 = table0.to_columnar();
   ColumnarTable input1 = table1.to_columnar();
   ColumnarTable input2 = table2.to_columnar();
   ColumnarTable input3 = table3.to_columnar();
   ColumnarTable input4 = table4.to_columnar();
   ColumnarTable input5 = table5.to_columnar();
   ColumnarTable input6 = table6.to_columnar();

   plan.inputs.emplace_back(std::move(input0));
   plan.inputs.emplace_back(std::move(input1));
   plan.inputs.emplace_back(std::move(input2));
   plan.inputs.emplace_back(std::move(input3));
   plan.inputs.emplace_back(std::move(input4));
   plan.inputs.emplace_back(std::move(input5));
   plan.inputs.emplace_back(std::move(input6));

   plan.root = 12; // final join node id

   auto* context = Contest::build_context();
   auto result = Contest::execute(plan, context);
   Contest::destroy_context(context);

   auto result_table = Table::from_columnar(result);
   // Expected final rows: only Table0's row {10,111} qualifies.
   // Combinations:
   //   Table2: (100,200) or (100,210)
   //   Table4: (300,400) or (300,410)
   //   Table6: (500,600) or (500,610)
   // Total: 2*2*2 = 8 rows.
   std::vector<std::vector<Data>> ground_truth{
      { 10, 111, 10, 100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600 },
      { 10, 111, 10, 100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 610 }
   };
   sort(result_table.table());
   sort(ground_truth.begin(), ground_truth.end());
   REQUIRE(result_table.table() == ground_truth);
}
