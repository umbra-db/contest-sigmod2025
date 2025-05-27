#include "query/PlanImport.hpp"
#include "infra/UnionFind.hpp"
#include <plan.h>
#include <table.h>
//---------------------------------------------------------------------------
namespace engine {
//-------------------------------------------------------------------------
namespace {
//-------------------------------------------------------------------------
struct PlanImporter {
    DataSource& dataSource;
    QueryPlan& result;
    const Plan& plan;
    UnionFind attrGroups;
    size_t attrCount = 0;

    struct Subtree {
        SmallVec<unsigned> attributes;
    };

    PlanImporter(DataSource& ds, QueryPlan& r, const Plan& p)
        : dataSource(ds), result(r), plan(p) {}

    Subtree rec(const PlanNode& n) {
        Subtree s;
        if (auto* join = std::get_if<JoinNode>(&n.data)) {
            Subtree left = rec(plan.nodes[join->left]);
            Subtree right = rec(plan.nodes[join->right]);
            for (auto [idx, _] : n.output_attrs) {
                if (idx < left.attributes.size()) {
                    s.attributes.push_back(left.attributes[idx]);
                } else {
                    s.attributes.push_back(right.attributes[idx - left.attributes.size()]);
                }
            }
            unsigned leftJoinAttr = left.attributes[join->left_attr];
            unsigned rightJoinAttr = right.attributes[join->right_attr];

            attrGroups.merge(leftJoinAttr, rightJoinAttr);
        } else if (auto* scan = std::get_if<ScanNode>(&n.data)) {
            BitSet attrs;
            auto* tbl = &dataSource.relations[scan->base_table_id];
            // If the table has no rows, it comes with no column structures, which is not nice
            bool fillTable = tbl->columns.empty();
            for (auto [idx, dt] : n.output_attrs) {
                s.attributes.push_back(attrCount);
                attrs.insert(attrCount);
                if (fillTable)
                    tbl->columns.push_back(DataSource::Column{dt, {}});
                result.addAttribute(scan->base_table_id, idx, dt);
                attrCount++;
            }
            assert(tbl);
            result.addInput(*tbl, attrs);
        }
        return s;
    }
};
//-------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
QueryPlan PlanImport::importPlanExistingData(DataSource& dataSource, const Plan& plan) {
    using namespace std;
    QueryPlan result{dataSource};

    PlanImporter importer(dataSource, result, plan);

    auto res = importer.rec(plan.nodes[plan.root]);


    UnorderedMap<unsigned, BitSet> eqSets;
    for (unsigned i = 0; i < importer.attrCount; i++) {
        unsigned eqClass = importer.attrGroups.find(i);
        eqSets[eqClass].insert(i);
    }
    SmallVec<BitSet> equivalenceSets;
    for (auto& [k, v] : eqSets)
        equivalenceSets.push_back(v);

    result.prepare(std::move(equivalenceSets));

    SmallVec<unsigned> attrs;
    attrs.reserve(res.attributes.size());
    for (unsigned a : res.attributes)
        attrs.push_back(a);
    result.setOutput(attrs);

    return result;
}
//---------------------------------------------------------------------------
DataSource::Table PlanImport::importTable(const ColumnarTable& input) {
    DataSource::Table table;
    table.numRows = input.num_rows;
    for (const auto& column : input.columns) {
        DataSource::Column c;
        c.type = column.type;
        c.pages = engine::span(reinterpret_cast<DataSource::Page* const*>(column.pages.data()), column.pages.size());
        table.columns.push_back(c);
    }
    return table;
}
//---------------------------------------------------------------------------
QueryPlan PlanImport::importPlan(DataSource& dataSource, const Plan& plan) {
    for (const auto& input : plan.inputs) {
        dataSource.relations.push_back(importTable(input));
    }

    return importPlanExistingData(dataSource, plan);
}
//---------------------------------------------------------------------------
namespace {
struct ActualTable : PlanImport::TableResult {
    ColumnarTable ct;

    ActualTable(ColumnarTable ct) : ct(std::move(ct)) {}
};
}
//---------------------------------------------------------------------------
std::unique_ptr<PlanImport::TableResult> PlanImport::makeTable(std::vector<std::vector<Data>> data, std::vector<DataType> types)
{
    Table table(std::move(data), std::move(types));
    auto ct = std::make_unique<ActualTable>(table.to_columnar());
    ct->table = importTable(ct->ct);
    return ct;
}
//---------------------------------------------------------------------------
}
