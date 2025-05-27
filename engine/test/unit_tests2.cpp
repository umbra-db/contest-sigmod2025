#include "attribute.h"
#include "infra/Random.hpp"
#include "infra/Scheduler.hpp"
#include "op/Hashtable.hpp"
#include "op/ScanBase.hpp"
#include "op/TableScan.hpp"
#include "op/TableTarget.hpp"
#include "pipeline/JoinPipeline.hpp"
#include "query/PlanImport.hpp"
#include "query/RuntimeValue.hpp"
#include "storage/StringPtr.hpp"
#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <mutex>
#include <set>
#include <variant>
#include <plan.h>
#include <table.h>

using namespace std;

/*
TEST_CASE("Scheduler") {
    atomic<size_t> numWorkers{0};
    atomic<size_t> numIncrements{0};
    engine::Scheduler::run([&](size_t workerId) {
        numWorkers.fetch_add(1);
        for (size_t i = 0; i < 10000; ++i) {
            numIncrements.fetch_add(1);
        }
    });
    REQUIRE(numWorkers.load() > 1);
}
*/

using namespace engine;

namespace {

struct ValueScanner : ScanImpl<ValueScanner> {
    std::vector<vector<uint64_t>> values;
    ValueScanner(std::vector<vector<uint64_t>> values) : values(std::move(values)) {}
    struct LocalState {
        LocalState(ValueScanner&) {}
    };
    template <typename LS, typename ConsumerType, typename Prepare, typename Init>
    void operator()(LS&& localStateFun, ConsumerType&& consumer, Prepare&&, Init&& init) {
        auto* ls = localStateFun(0);
        init(0, ls, true);
        for (auto& row : values) {
            consumer(localStateFun(0), [&](unsigned idx) { return row[idx]; });
        }
        init(0, ls, false);
    }
};

struct ValueCollector : TargetImpl<ValueCollector> {
    struct LocalState {
        LocalState(ValueCollector&) {}
    };
    std::vector<vector<uint64_t>> values;
    template <typename... AttrT>
    void operator()(LocalState&, uint64_t multiplicity, AttrT... attrs) {
        for (size_t i = 0; i < multiplicity; i++) {
            vector<uint64_t> row;
            (row.emplace_back(attrs), ...);
            values.push_back(std::move(row));
        }
    }
};

struct MockJoin {
    struct LocalState {
        LocalState(MockJoin&) {}
    };
    /// Returns 2 copies of the key + idx
    template <typename ConsumerType, typename = std::enable_if_t<Consumer<ConsumerType>>>
    void operator()(LocalState&, uint64_t key, ConsumerType&& consumer) {
        for (size_t i = 0; i < 2; i++)
            consumer([key](unsigned idx) {
                if constexpr (config::handleMultiplicity)
                    return idx == 0 ? 1 : key + (idx - 1);
                else
                    return key + idx;
            });
    }
    void prepare(LocalState&, uint64_t) {}
};

}

namespace engine::test {
struct TestScan : ScanImpl<TestScan> {
    struct LocalState {};
    template <typename LS, typename ConsumerType, typename Prepare>
    void operator()(LS* ls, ConsumerType&& consumer, Prepare&&) {
        consumer(ls[0], [](unsigned) { return 0; });
    }
};
}

TEST_CASE("Reflection") {
    engine::test::TestScan ts{};
    REQUIRE(static_cast<ScanBase&>(ts).getName() == "engine::test::TestScan");
}

struct ContextWrapper {
    void* context;
    ContextWrapper() {
        context = Contest::build_context();
        Scheduler::start_query();
    }
    ~ContextWrapper() {
        Scheduler::end_query();
        Contest::destroy_context(context);
    }
};

TEST_CASE("JoinPipeline") {
    ContextWrapper context{};

    ValueScanner scanner{
        {{1, 2}, {10, 11}, {20, 21}}};
    ValueCollector collector;
    JoinPipeline<ValueCollector, ValueScanner, std::tuple<MockJoin, MockJoin>, std::index_sequence<0, 1>, std::index_sequence<0, 0, 1, 2>> pipeline(collector, scanner, {MockJoin{}, MockJoin{}}, {0, 1}, {0, 1, 2, 2});

    pipeline();

    REQUIRE(collector.values ==
            std::vector<std::vector<uint64_t>>{{1, 2, 3, 4}, {1, 2, 3, 4}, {1, 2, 3, 4}, {1, 2, 3, 4}, {10, 11, 12, 13}, {10, 11, 12, 13}, {10, 11, 12, 13}, {10, 11, 12, 13}, {20, 21, 22, 23}, {20, 21, 22, 23}, {20, 21, 22, 23}, {20, 21, 22, 23}});
}

// generate new tuples
// genKey takes the number of the tuple and generates the key of the tuple
// genFct takes the number of the tuple and the attribute number and generates the attribute value
static vector<vector<uint64_t>> generateTuple(size_t numElems, size_t attr, const std::function<uint64_t(size_t)>& genKey, const std::function<uint64_t(size_t, size_t)>& genFct) {
    vector<vector<uint64_t>> res;
    for (size_t i = 0; i < numElems; i++) {
        vector<uint64_t> tuple(attr, 0);
        tuple[0] = genKey(i);
        for (size_t index = 1; index < attr; index++) {
            tuple[index] = genFct(i, index);
        }
        res.push_back(tuple);
    }
    return res;
}
// perform a cross join between the two given relation
// return a vector of vectors of joined tuples (the join key is not duplicated)
// vR1: relation 1
// vR2: relation 2
// keyAttrR1: number of the key attribute in relation vR1 (0 based)
// keyAttrR2: number of the key attribute in relation vR2 (0 based)
static vector<vector<uint64_t>> doCrossJoin(vector<vector<uint64_t>>& vR1, vector<vector<uint64_t>>& vR2, size_t keyAttrR1, size_t keyAttrR2) {
    vector<vector<uint64_t>> res;
    for (auto& tupleR1 : vR1) {
        for (auto& tupleR2 : vR2) {
            if (tupleR1[keyAttrR1] == tupleR2[keyAttrR2]) {
                vector<uint64_t> tuple;
                tuple.insert(tuple.end(), tupleR1.begin(), tupleR1.end());
                for (size_t i = 0; i < tupleR2.size(); i++) {
                    if (i != keyAttrR2) tuple.push_back(tupleR2[i]);
                }
                res.push_back(tuple);
            }
        }
    }
    return res;
}

TEST_CASE("HashTable") {
    ContextWrapper context{};

    // fill the hash table by running the build side
    using HT = Hashtable;
    HT ht2;
    CHECK(ht2.isEmpty());
    HashtableBuild ht2build(ht2, 16);

    auto vR0 = generateTuple(1000, 2, [](size_t index) { return index + 1; }, [](size_t index, size_t) { return static_cast<uint64_t>(100 * std::pow(10, index % 4)); });

    ValueScanner R0{vR0};
    ValueCollector result;

    JoinPipeline<HashtableBuild, ValueScanner, std::tuple<>, std::index_sequence<>, std::index_sequence<0, 0>> pipelineScan1(ht2build, R0, {}, {}, {0, 1});
    pipelineScan1();

    // ht2 is populated, so now look up the keys in the bloom filter
    std::set<uint64_t> keys;
    for (auto& tuple : vR0) {
        CHECK(ht2.joinFilter(tuple[0]));
        CHECK(ht2.joinFilterPrecise(tuple[0]));
        keys.insert(tuple[0]);
    }

    // generate 1000 random values not in the ht
    std::set<uint64_t> notInHT;
    Random r;
    for (size_t i = 0; i < 1000; i++) {
        auto number = r();
        while (keys.count(number)) number = r();
        notInHT.insert(number);
    }

    // check that the generated random number are not in the ht
    for (auto& n : notInHT) {
        CHECK(!ht2.joinFilterPrecise(n));
    }
}

TEST_CASE("HashJoin") {
    ContextWrapper context{};

    // test different relation sizes
    std::vector<size_t> sizes = {1, 2, 5, 100, 200, 1000};

    for (auto a : sizes) {
        for (auto b : sizes) {
            for (auto c : sizes) {
                std::cout << a << " " << b << " " << c << std::endl;
                using HT = Hashtable;
                HT ht1, ht2;
                CHECK(ht1.isEmpty());
                CHECK(ht2.isEmpty());
                HashtableBuild ht1build(ht1, 16);
                HashtableBuild ht2build(ht2, 16);

                auto vR0 = generateTuple(a, 2, [](size_t index) { return index + 1; }, [](size_t index, size_t) { return static_cast<uint64_t>(100 * std::pow(10, index % 4)); });
                auto vR1 = generateTuple(b, 2, [](size_t index) { return index + 1; }, [](size_t index, size_t) { return 10 * index; });
                auto vR2 = generateTuple(c, 2, [](size_t index) { return static_cast<uint64_t>(100 * std::pow(10, index % 4)); }, [](size_t index, size_t attr) { return 10 * index + attr; });

                ValueScanner R0{vR0};
                ValueScanner R1{vR1};
                ValueScanner R2{vR2};
                ValueCollector result;

                JoinPipeline<HashtableBuild, ValueScanner, std::tuple<>, std::index_sequence<>, std::index_sequence<0, 0>> pipelineScan1(ht2build, R2, {}, {}, {0, 1});
                pipelineScan1();

                JoinPipeline<HashtableBuild, ValueScanner, std::tuple<>, std::index_sequence<>, std::index_sequence<0, 0>> pipelineScan2(ht1build, R1, {}, {}, {0, 1});
                pipelineScan2();

                JoinPipeline<ValueCollector, ValueScanner, std::tuple<HashtableProbe, HashtableProbe>, std::index_sequence<0, 0>, std::index_sequence<0, 0, 1, 2>> pipelineJoin(result, R0, {HashtableProbe{&ht1}, HashtableProbe{&ht2}}, {0, 1}, {0, 1, 1, 1});
                pipelineJoin();

                // join R0 and R1 on the first attribute
                auto vR0vR1 = doCrossJoin(vR0, vR1, 0, 0);
                // continue join with R2 on the second/first column respectively
                auto vR0vR1vR2 = doCrossJoin(vR0vR1, vR2, 1, 0);
                sort(vR0vR1vR2.begin(), vR0vR1vR2.end());

                sort(result.values.begin(), result.values.end());

                REQUIRE(result.values == vR0vR1vR2);
            }
        }
    }
}

namespace {

template <typename _To, typename _From>
[[nodiscard]] constexpr _To bit_cast_helper(const _From& __from) noexcept {
    return __builtin_bit_cast(_To, __from);
}

struct TypedCollector {
    engine::span<const DataType> types;
    std::vector<std::vector<PlanImport::Data>> values;
    std::mutex values_mutex;

    static PlanImport::Data convert(uint64_t val, DataType type) {
        if (val == TableScan::nullValue)
            return std::monostate{};
        switch (type) {
            case DataType::INT32: return int32_t(val);
            case DataType::INT64: return int64_t(val);
            case DataType::FP64: return bit_cast_helper<double>(val);
            case DataType::VARCHAR: {
                auto str = StringPtr{val};
                return str.materialize_string();
            }
        }
        __builtin_unreachable();
    }

    template <typename T, typename ProviderT>
    void operator()(T&, ProviderT provider) {
        vector<PlanImport::Data> row;
        std::scoped_lock lock(values_mutex);
        for (size_t i = 0; i < types.size(); i++) {
            row.push_back(convert(provider(i), types[i]));
        }
        values.push_back(std::move(row));
    }
};
}

template <size_t NumAttrs>
void testTableScan(const vector<DataType>& types, vector<vector<PlanImport::Data>> original) {
    struct LS {
        TableScan::LocalState scan;

        LS(TableScan& scan) : scan(scan) {}
    };
    auto initLS = [](TableScan& scan) {
        vector<LS> ls;
        unsigned sz = scan.concurrency();
        ls.reserve(sz);
        for (unsigned i = 0; i < sz; i++)
            ls.emplace_back(scan);
        return ls;
    };
    auto compare = [](const auto& lhs, const auto& rhs) {
        assert(lhs.size() == rhs.size());
        for (size_t i = 0; i < lhs.size(); i++) {
            if (lhs[i] != rhs[i])
                return lhs[i] < rhs[i];
        }
        return false;
    };
    auto printVec = [](vector<vector<PlanImport::Data>>& vec) {
    };

    std::sort(original.begin(), original.end(), compare);
    auto tbl = PlanImport::makeTable(original, types);
    {
        TypedCollector collector{types, {}};
        auto tblInfo = TableScan::makeTableInfo(tbl->table);
        TableScan scan(tblInfo);
        auto ls = initLS(scan);
        scan([&](size_t workerId) { return &ls[workerId]; }, collector, [](auto, auto) {}, [&](auto, auto, auto) {});

        std::sort(collector.values.begin(), collector.values.end(), compare);
        // print
        if (original != collector.values) {
            printVec(original);
            printVec(collector.values);
        }
        REQUIRE(original == collector.values);
    }
    {
        SmallVec<DataType> typesSmall;
        for (auto i = 0; i < types.size(); i++) typesSmall.emplace_back(types[i]);
        TableTarget target(std::move(typesSmall));
        auto tblInfo = TableScan::makeTableInfo(tbl->table);
        TableScan scan(tblInfo);

        struct LS2 {
            TableScan::LocalState scan;
            TableTarget::LocalState target;

            LS2(TableTarget& target, TableScan& scan) : scan(scan), target(target) {}
        };
        vector<LS2> ls;
        unsigned sz = Scheduler::concurrency();
        ls.reserve(sz);
        for (unsigned i = 0; i < sz; i++)
            ls.emplace_back(target, scan);

        scan([&](size_t workerId) { return &ls[workerId]; }, [&](auto& ls, auto&& provider) {
            static_assert(Provider<decltype(provider)>);
            ([&]<size_t... I>(std::index_sequence<I...>) {
                target(reinterpret_cast<LS2*>(ls)->target, 1, provider(I)...);
            })(std::make_index_sequence<NumAttrs>{}); }, [](auto, auto) {},
             [&](size_t, void* ls, bool init) { if(!init)target.finalize(reinterpret_cast<LS2*>(ls)->target); });
        target.finishConsume();

        auto ct = target.extract();
        auto tbl2 = PlanImport::importTable(ct);
        TypedCollector collector{types, {}};
        auto tblInfo2 = TableScan::makeTableInfo(tbl2);
        TableScan scan2(tblInfo2);
        auto ls2 = initLS(scan2);
        scan2([&](size_t workerId) { return &ls2[workerId]; }, collector, [](auto, auto) {}, [](auto, auto, auto) {});

        std::sort(collector.values.begin(), collector.values.end(), compare);
        // print
        if (original != collector.values) {
            printVec(original);
            printVec(collector.values);
        }

        REQUIRE(original.size() == collector.values.size());
        REQUIRE(original == collector.values);
        target.localStates.clear();
    }
}

TEST_CASE("TableScan") {
    ContextWrapper context{};

    SECTION("base") {
        testTableScan<2>(
            {DataType::INT32, DataType::VARCHAR},
            {{5, "a"},
             {6, "b"},
             {7, "c"}});
    }
    SECTION("nulls") {
        testTableScan<2>(
            {DataType::INT32, DataType::VARCHAR},
            {{std::monostate{}, "a"},
             {6, "b"},
             {7, std::monostate{}}});
        string longstr(32'000, 'a');
    }
    SECTION("long varchar") {
        string longstr(32'000, 'a');
        testTableScan<2>(
            {DataType::INT32, DataType::VARCHAR},
            {{5, "a"},
             {6, longstr},
             {7, "c"}});
    }
    // Vector with many values
    /*SECTION("many values") {
        vector<vector<PlanImport::Data>> data{
            {std::monostate{}, "a"},
            {6, "b"},
            {7, std::monostate{}}};
        for (int i = 0; i < 8'000; i++) {
            data.push_back({PlanImport::Data{i}, PlanImport::Data{engine::fmt::format("{}", i)}});
        }
        testTableScan<2>({DataType::INT32, DataType::VARCHAR}, data);
    }*/
}

TEST_CASE("TableTarget") {
    ContextWrapper context{};

    SECTION("integers") {
        using T = int32_t;
        TableTarget tt({DataType::INT32});
        TableTarget::LocalState ls(tt);
        SECTION("single page") {
            tt(ls, 1, 42ull);
            tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            REQUIRE(table.columns.front().pages.size() == 1);

            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 2);
            REQUIRE(data.front().size() == 1);
            REQUIRE(std::holds_alternative<T>(data[0].front()));
            REQUIRE(std::get<T>(data[0].front()) == 42);
            REQUIRE(std::holds_alternative<std::monostate>(data[1].front()));
        }
        SECTION("fully filled page (values only)") {
            for (unsigned i = 0; i < 1984; i++) tt(ls, 1, i);
            //REQUIRE(tt.localStateRefs.load()->writers.front()->pages.size() == 1);
            tt(ls, 1, 42);
            ls.flushBuffers();
            //REQUIRE(tt.localStateRefs.load()->writers.front()->pages.size() == 2);
            tt.finalize(ls);
            tt.finishConsume();
            tt.extract();
        }
        SECTION("many items") {
            for (unsigned i = 0; i < 10'000; i++) tt(ls, 1, i);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 10'000);
            unsigned i = 0;
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == i++);
            }
        }
        SECTION("many nulls") {
            for (unsigned i = 0; i < 10'000; i++) tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 10'000);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<std::monostate>(elem.front()));
            }
        }
        SECTION("constants") {
            constexpr size_t num = 200'000;
            tt.localStateRefs.load()->numRows = num;
            tt.finalize(ls);
            tt.finishConsume();
            RuntimeValue rv = RuntimeValue::from(DataType::INT32, 42);
            const auto table = tt.prepareAndExtract({rv});
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == num);
            REQUIRE(data.front().size() == 1);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == 42);
            }
        }
        SECTION("duplicate column") {
            tt(ls, 1, 42ull);
            tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.prepareAndExtract({0u, 0u});
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 2);
            REQUIRE(data.front().size() == 2);
            REQUIRE(std::get<T>(data[0][0]) == 42);
            REQUIRE(std::get<T>(data[0][1]) == 42);
            REQUIRE(std::holds_alternative<std::monostate>(data[1][0]));
            REQUIRE(std::holds_alternative<std::monostate>(data[1][1]));
        }
        tt.localStates.clear();
    }
    SECTION("i64/fp64") {
        using T = int64_t;
        TableTarget tt({DataType::INT64});
        TableTarget::LocalState ls(tt);
        SECTION("single page") {
            tt(ls, 1, 42ull);
            tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            REQUIRE(table.columns.front().pages.size() == 1);

            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 2);
            REQUIRE(data.front().size() == 1);
            REQUIRE(std::holds_alternative<T>(data[0].front()));
            REQUIRE(std::get<T>(data[0].front()) == 42);
            REQUIRE(std::holds_alternative<std::monostate>(data[1].front()));
        }
        SECTION("fully filled page (values only)") {
            for (unsigned i = 0; i < 1007; i++) tt(ls, 1, i);
            //REQUIRE(tt.localStateRefs.load()->writers.front()->pages.size() == 1);
            tt(ls, 1, 42);
            ls.flushBuffers();
            //REQUIRE(tt.localStateRefs.load()->writers.front()->pages.size() == 2);
            tt.finalize(ls);
            tt.finishConsume();
            tt.extract();
        }
        SECTION("many items") {
            for (unsigned i = 0; i < 10'000; i++) tt(ls, 1, i);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 10'000);
            unsigned i = 0;
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == i++);
            }
        }
        SECTION("many nulls") {
            for (unsigned i = 0; i < 10'000; i++) tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 10'000);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<std::monostate>(elem.front()));
            }
        }
        SECTION("constants") {
            constexpr size_t num = 200'000;
            tt.localStateRefs.load()->numRows = num;
            tt.finalize(ls);
            tt.finishConsume();
            RuntimeValue rv = RuntimeValue::from(DataType::INT64, 42);
            const auto table = tt.prepareAndExtract({rv});
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == num);
            REQUIRE(data.front().size() == 1);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == 42);
            }
        }
        tt.localStates.clear();
    }
    SECTION("strings") {
        using T = std::string;
        TableTarget tt({DataType::VARCHAR});
        TableTarget::LocalState ls(tt);
        DataSource::Page pages[6];
        for (auto& page : pages) {
            page.numRows = 0xfffe;
            page.numNotNull = DataSource::PAGE_SIZE - 4;
            memset(page.getLongString(), 'a', page.numNotNull);
        }
        pages[0].numRows = 0xffff;
        pages[5].numNotNull = 1234;
        DataSource::Page* page_ptr[6];
        for (unsigned i = 0; i < 6; i++)
            page_ptr[i] = &pages[i];
        StringPtr long_string = StringPtr::fromLongString(page_ptr, 6);
        std::string val = "abc";
        StringPtr short_string = StringPtr::fromString(val.data(), val.size());
        SECTION("single page") {
            tt(ls, 1, short_string.val());
            tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            REQUIRE(table.columns.front().pages.size() == 1);

            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 2);
            REQUIRE(data.front().size() == 1);
            REQUIRE(std::holds_alternative<T>(data[0].front()));
            REQUIRE(std::get<T>(data[0].front()) == val);
            REQUIRE(std::holds_alternative<std::monostate>(data[1].front()));
        }
        SECTION("long string") {
            tt(ls, 1, RuntimeValue::nullValue);
            tt(ls, 1, long_string.val());
            tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            REQUIRE(table.columns.front().pages.size() == 8);

            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 3);
            REQUIRE(data.front().size() == 1);
            REQUIRE(std::holds_alternative<std::monostate>(data[0].front()));
            REQUIRE(std::holds_alternative<T>(data[1].front()));
            REQUIRE(std::get<T>(data[1].front()) == std::string(42'174, 'a'));
            REQUIRE(std::holds_alternative<std::monostate>(data[2].front()));
        }
        SECTION("many long strings") {
            tt(ls, 1, RuntimeValue::nullValue);
            tt(ls, 5, long_string.val());
            tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();

            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 7);
            REQUIRE(data.front().size() == 1);
            REQUIRE(std::holds_alternative<std::monostate>(data[0].front()));
            for (int i = 0; i < 5; i++) {
                REQUIRE(std::holds_alternative<T>(data[1 + i].front()));
                REQUIRE(std::get<T>(data[1 + i].front()) == std::string(42'174, 'a'));
            }
            REQUIRE(std::holds_alternative<std::monostate>(data[6].front()));
        }
        SECTION("many items") {
            for (unsigned i = 0; i < 10'000; i++) tt(ls, 1, short_string.val());
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 10'000);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == val);
            }
        }
        SECTION("many nulls") {
            for (unsigned i = 0; i < 10'000; i++) tt(ls, 1, RuntimeValue::nullValue);
            tt.finalize(ls);
            tt.finishConsume();
            const auto table = tt.extract();
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == 10'000);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<std::monostate>(elem.front()));
            }
        }
        SECTION("short string constants") {
            constexpr size_t num = 200'000;
            tt.localStateRefs.load()->numRows = num;
            tt.finalize(ls);
            tt.finishConsume();
            StringPtr str = StringPtr::fromString("42");
            RuntimeValue rv = RuntimeValue::from(DataType::VARCHAR, str.val());
            const auto table = tt.prepareAndExtract({rv});
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == num);
            REQUIRE(data.front().size() == 1);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == "42");
            }
        }
        SECTION("mid string constants") {
            constexpr size_t num = 200'000;
            tt.localStateRefs.load()->numRows = num;
            tt.finalize(ls);
            tt.finishConsume();
            StringPtr str = StringPtr::fromString("12345678909876896578");
            RuntimeValue rv = RuntimeValue::from(DataType::VARCHAR, str.val());
            const auto table = tt.prepareAndExtract({rv});
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == num);
            REQUIRE(data.front().size() == 1);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == "12345678909876896578");
            }
        }
        SECTION("long string constants") {
            constexpr size_t num = 200;
            tt.localStateRefs.load()->numRows = num;
            tt.finalize(ls);
            tt.finishConsume();
            RuntimeValue rv = RuntimeValue::from(DataType::VARCHAR, long_string.val());
            const auto table = tt.prepareAndExtract({rv});
            const auto data = Table::from_columnar(table).table();
            REQUIRE(data.size() == num);
            REQUIRE(data.front().size() == 1);
            for (const auto& elem : data) {
                REQUIRE(std::holds_alternative<T>(elem.front()));
                REQUIRE(std::get<T>(elem.front()) == std::string(42'174, 'a'));
            }
        }
        tt.localStates.clear();
    }
}
