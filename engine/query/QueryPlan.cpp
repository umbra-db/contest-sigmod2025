#include "query/QueryPlan.hpp"
#include "infra/Scheduler.hpp"
#include "infra/SmallVec.hpp"
#include "op/CollectorTarget.hpp"
#include "op/Hashtable.hpp"
#include "op/TableScan.hpp"
#include "op/TableTarget.hpp"
#include "pipeline/PipelineFunction.hpp"
#include "query/QueryGraph.hpp"
#include "storage/RestrictionLogic.hpp"
#include <chrono>
#include <plan.h>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// An attribute corresponding to a physical column
struct QueryPlan::Attribute {
    unsigned relation;
    unsigned column;
    DataType dataType;
    unsigned eqClass;
    TableScan::ColumnInfo info;
};
//---------------------------------------------------------------------------
/// An input. It is either a base table or a hash table built on an attribute
struct QueryPlan::Input {
    /// The equivalence classes being produced
    BitSet producedEq;
    /// The cardinality estimation
    double cardinality = 1.0;

    /// The hash table
    UniquePtr<Hashtable> ht;
    /// The hash table build
    UniquePtr<HashtableBuild> htBuild;
    /// The restriction based on this hash table
    UniquePtr<RestrictionLogic> restrictionLogic;
    /// The additional restrictions if this was a singleton
    SmallVec<UniquePtr<RestrictionLogic>> additionalRestrictionLogics;
    /// The equivalence classes to their offsets in the storage
    UnorderedMap<unsigned, unsigned> eqOffsets;
    /// The key the hash table is built on. This eq must have offset 0
    unsigned keyEq = ~0u;
    /// Is this a cross product?
    bool isCrossProduct = false;
    /// The source scan, only used for debugging
    Input* sourceScan = nullptr;
    /// The source probes, only used for debugging
    SmallVec<Input*> sourceProbes;

    /// The attributes being produced
    BitSet producedAttributes;
    /// The table being scanned
    DataSource::Table* table = nullptr;
    /// The table info
    TableScan::TableInfo tableInfo;
    /// Random sample of up to 64 rows, columnwise. These may only be uint32_t attributes
    Vector<uint32_t> sample;
    /// Eq class to sample column
    UnorderedMap<unsigned, unsigned> sampleOffsets;
    /// The size of the sample
    size_t sampleSize = 0;
    /// The currently matching tuples in the sample
    uint64_t sampleMatches = ~0ull;

    /// Recompute cardinality from sample
    void recomputeCardinality() {
        assert(isBase());
        auto matches = engine::popcount(sampleMatches);
        double selectivity = matches ? double(matches) / sampleSize : (1.0 / (sampleSize * 2));
        cardinality = table->numRows * selectivity;
    }

    /// Is this a base table or hash table?
    bool isBase() const { return table != nullptr; }
    /// Get unfiltered row count
    size_t getUnfilteredRows() const {
        if (isBase())
            return table->numRows;
        else
            return ht->getNumTuples();
    }
    /// Is singleton?
    bool isSingleton() const {
        return getUnfilteredRows() <= 1;
    }
    /// The number of columns
    size_t getCols() const {
        return eqOffsets.size();
    }
};
//---------------------------------------------------------------------------
QueryPlan::QueryPlan(DataSource& db) : db(&db) {}
QueryPlan::~QueryPlan() noexcept = default;
QueryPlan::QueryPlan(QueryPlan&& other) noexcept = default;
QueryPlan& QueryPlan::operator=(QueryPlan&& other) noexcept = default;
//---------------------------------------------------------------------------
TableScan QueryPlan::buildScan(Input& input, BitSet requiredEqs, double mult) {
    assert(input.isBase());
    BitSet eqs = input.producedEq & requiredEqs;

    // Find matching restrictions
    SmallVec<TableScan::RestrictionInfo> restrictions;

    // Find the actual attributes that correspond to the equivalence classes
    SmallVec<unsigned> colsVec;
    for (unsigned eq : eqs) {
        auto attr = input.producedAttributes & equivalenceSets[eq];
        assert(attr.size() == 1);
        unsigned col = input.producedAttributes.getIndex(attr.front());
        colsVec.push_back(col);
    }
    for (unsigned eq : input.producedEq) {
        auto attr = input.producedAttributes & equivalenceSets[eq];
        assert(attr.size() == 1);
        unsigned col = input.producedAttributes.getIndex(attr.front());
        if (auto it = eqRestrictions.find(eq); it != eqRestrictions.end()) {
            double selectivity = 1.0;
            // Sample may not be prepared yet, we might be eliminating singletons
            if (!input.sample.empty()) {
                auto off = input.sampleOffsets.at(eq);
                auto matches = it->second->run(input.sample.data() + off, ~0ull >> (64 - input.sampleSize));
                selectivity = matches ? double(engine::popcount(matches)) / input.sampleSize : (1.0 / (input.sampleSize * 2));
            }
            restrictions.push_back(TableScan::RestrictionInfo{col, selectivity, it->second});
        }
    }

    return TableScan(input.tableInfo, colsVec, restrictions, mult, double(input.cardinality) / input.tableInfo.numRows);
}
//---------------------------------------------------------------------------
void QueryPlan::estimateCardinality(Input& input) {
    if (!input.isBase()) {
        assert(input.ht);
        input.cardinality = double(input.ht->getNumTuples());
        return;
    }
    // TODO: We want to estimate based on the sample in the future

    // Multiple restriction selectivities with dampening
    SmallVec<double, 8> sels;
    for (unsigned eq : input.producedEq) {
        if (eqRestrictions.find(eq) != eqRestrictions.end()) {
            auto& r = *eqRestrictions.at(eq);
            sels.push_back(r.estimateSelectivity());
        }
    }
    std::sort(sels.begin(), sels.end());

    double totalSel = 1.0;
    double dampening = 1.0;
    for (auto& s : sels) {
        // The factor should get close to 1 over time
        totalSel *= 1.0 - (1.0 - s) * dampening;
        dampening *= 0.5;
    }

    input.cardinality = std::max(input.table->numRows * totalSel, 0.5);
}
//---------------------------------------------------------------------------
struct QueryEmptyException : public std::exception {
    const char* what() const noexcept override { return "Query is empty"; }
};
//---------------------------------------------------------------------------
void QueryPlan::addInput(DataSource::Table& table, engine::BitSet attrs) {
    auto input = makeUnique<Input>();
    input->table = &table;
    input->producedAttributes = attrs;
    inputs.push_back(std::move(input));
}
//---------------------------------------------------------------------------
void QueryPlan::addAttribute(unsigned relation, unsigned column, DataType dataType) {
    attributes.push_back({relation, column, dataType, 0, {}});
}
//---------------------------------------------------------------------------
void QueryPlan::prepare(SmallVec<BitSet> inputEqs) {
    equivalenceSets = std::move(inputEqs);
    for (size_t eqClass = 0; eqClass < equivalenceSets.size(); eqClass++) {
        for (unsigned attr : equivalenceSets[eqClass]) {
            assert(attr < attributes.size());
            attributes[attr].eqClass = eqClass;
        }
    }
    for (auto& input : inputs) {
        for (unsigned attr : input->producedAttributes) {
            assert(attr < attributes.size());
            input->producedEq.insert(attributes[attr].eqClass);
        }
        input->tableInfo.numRows = input->table->numRows;
        input->tableInfo.name = input->table->name;
        input->tableInfo.columns.reserve(8);
        for (unsigned attr : input->producedAttributes)
            input->tableInfo.columns.push_back(&attributes[attr].info);
    }
    Scheduler::parallelFor(0, attributes.size(), [&](size_t workerId, size_t attrInd) {
        auto& attr = attributes[attrInd];
        attr.info = TableScan::prepareColumn(db->relations[attr.relation].numRows, db->relations[attr.relation].columns[attr.column]);
    });
}
//---------------------------------------------------------------------------
void QueryPlan::setOutput(engine::span<const unsigned> attrs) {
    outputEqs.reserve(attrs.size());
    for (unsigned a : attrs) {
        assert(a < attributes.size() && "Have you prepared query plan?");
        outputEqs.push_back(attributes[a].eqClass);
    }
}
//---------------------------------------------------------------------------
struct QueryPlan::PlanPipeline {
    BitSet rels;
    double cost = std::numeric_limits<double>::infinity();
    unsigned keyEq = ~0u;
    unsigned scanInput = ~0u;
    struct ProbeInfo {
        unsigned probeInput;
        unsigned probeKeyEq;
    };
    SmallVec<ProbeInfo> probes;

    bool operator<(const PlanPipeline& other) const {
        return std::make_tuple(cost, probes.size()) < std::make_tuple(other.cost, other.probes.size());
    }
    operator bool() const {
        return cost != std::numeric_limits<double>::infinity();
    }
    // Is this the output pipeline
    bool isOutput() const {
        return keyEq == ~0u;
    }
};
//---------------------------------------------------------------------------
static constexpr unsigned crossProductEq = 63;
//---------------------------------------------------------------------------
struct QueryPlan::CheapestPipelineFinder {
    struct Subtree {
        PlanPipeline cheapestPipeline;
        PlanPipeline currentPipeline;
    };

    static Subtree rec(QueryGraph& qg, QueryGraph::Plan* cur) {
        Subtree result;
        if (cur->isLeaf()) {
            result.currentPipeline.rels = cur->set;
            result.currentPipeline.scanInput = cur->set.front();
            result.currentPipeline.cost = 0;
            return result;
        }
        auto left = rec(qg, cur->left);
        auto right = rec(qg, cur->right);
        result.cheapestPipeline = std::min(left.cheapestPipeline, right.cheapestPipeline);
        PlanPipeline leftPipeline;
        if (left.currentPipeline) {
            leftPipeline = left.currentPipeline;
            auto inters = (cur->left->eqs & cur->right->eqs);
            leftPipeline.keyEq = inters.empty() ? crossProductEq : inters.front();
            leftPipeline.cost = cur->left->cost + cur->left->card * 10;
            leftPipeline.rels = cur->left->set;
        }
        // If the left is not a hash table yet, we can consider it
        if (leftPipeline && qg.inputs[cur->left->set.front()].joinKey == ~0u)
            result.cheapestPipeline = std::min(result.cheapestPipeline, leftPipeline);
        // We are only interested in pipelines that probe leaves that are hash tables
        if (leftPipeline && cur->left->set.single() && qg.inputs[cur->left->set.front()].joinKey != ~0u) {
            // We would like to make the following assumption, but cross products make it very difficult
            //assert((leftPipeline.keyEq == crossProductEq) || (qg.inputs[cur->left->set.front()].joinKey == leftPipeline.keyEq));
            result.currentPipeline = right.currentPipeline;
            result.currentPipeline.probes.push_back(PlanPipeline::ProbeInfo{cur->left->set.front(), leftPipeline.keyEq});
            result.currentPipeline.rels = cur->set;
        }
        return result;
    }

    static PlanPipeline findCheapestPipeline(QueryGraph& qg, QueryGraph::Plan* root) {
        Subtree res = rec(qg, root);
        if (res.currentPipeline) {
            res.currentPipeline.cost = root->cost;
            res.cheapestPipeline = std::min(res.cheapestPipeline, res.currentPipeline);
        }
        return res.cheapestPipeline;
    }
};
//---------------------------------------------------------------------------
BitSet QueryPlan::computeRequiredEq(BitSet relations) {
    BitSet producedEqs;
    BitSet requiredEqs;
    for (unsigned i = 0; i < inputs.size(); i++) {
        if (relations.contains(i))
            producedEqs += inputs[i]->producedEq;
        else
            requiredEqs += inputs[i]->producedEq;
    }
    for (unsigned eq : outputEqs)
        requiredEqs.insert(eq);
    for (auto& [eq, v] : eqConstants)
        requiredEqs.erase(eq);
    return producedEqs & requiredEqs;
}
//---------------------------------------------------------------------------
static constexpr std::string_view joinText = " J ";
//---------------------------------------------------------------------------
static void writeStringAt(Vector<std::string>& out, std::string_view str, size_t x, size_t y) {
    if (out.size() <= y)
        out.resize(y + 1);
    if (out[y].size() <= x)
        out[y].resize(x + str.size() + 1, ' ');
    for (size_t i = 0; i < str.size(); i++)
        out[y][x + i] = str[i];
}
//---------------------------------------------------------------------------
static std::string_view getTableName(std::string_view res) noexcept {
    static std::unordered_map<std::string_view, std::string_view> tableNames{
        {"keyword", "k"},
        {"movie_keyword", "mk"},
        {"cast_info", "ci"},
        {"char_name", "chn"},
        {"comp_cast_type", "cct"},
        {"complete_cast", "cc"},
        {"kind_type", "kt"},
        {"link_type", "lt"},
        {"movie_link", "ml"},
        {"name", "n"},
        {"title", "t"},
        {"movie_info_idx", "midx"},
        {"movie_info", "mi"},
        {"role", "r"},
        {"person_info", "pi"},
        {"movie_companies", "mc"},
        {"company_name", "cn"},
        {"company_type", "ct"},
        {"role_type", "rt"},
        {"info_type", "it"},
        {"aka_name", "an"},
        {"aka_title", "at"},
    };

    // Take res until first '|'
    auto pos = res.find('|');
    if (pos != std::string_view::npos)
        res = res.substr(0, pos);

    return tableNames.at(res);
}
//---------------------------------------------------------------------------
namespace {
struct QPNode {
    QueryPlan::Input* cur = nullptr;
    size_t step = 0;

    bool isLeaf() const {
        return step < cur->sourceProbes.size();
    }

    QPNode left() const {
        return {cur->sourceProbes[cur->sourceProbes.size() - step - 1], 0};
    }
    QPNode right() const {
        return {cur, step + 1};
    }
    std::string leaf() const {
        return std::string{getTableName(cur->sourceScan->table->name)};
    }
};
struct QGNode {
    QueryPlan* plan = nullptr;
    QueryGraph::Plan* cur = nullptr;

    bool isLeaf() const {
        return cur->isLeaf();
    }

    QGNode left() const {
        return {plan, cur->left};
    }
    QGNode right() const {
        return {plan, cur->right};
    }
};
}
//---------------------------------------------------------------------------
template <typename Node>
static std::tuple<uint64_t, uint64_t, uint64_t> printPlanRec(Vector<std::string>& out, Node root, size_t baseX = 0, size_t baseY = 0) {
    size_t x = baseX;
    size_t y = baseY;

    if (!root.isLeaf()) {
        auto [nx, ny, topj] = printPlanRec(out, root.left(), x, y + 3);
        x = nx;
        auto jpos = x + joinText.size() / 2;
        writeStringAt(out, joinText, x, y);
        x += joinText.size();
        auto [nx2, ny2, topj2] = printPlanRec(out, root.right(), x, y + 3);
        x = nx2;

        writeStringAt(out, "|", topj, y + 2);
        writeStringAt(out, "|", topj2, y + 2);
        out[y + 1].resize(std::max(out[y + 1].size(), topj2 + 1), ' ');
        for (size_t i = topj; i <= topj2; i++)
            out[y + 1][i] = '_';
        out[y + 1][jpos] = '^';

        y += 3;
        return {x, y, jpos};
    } else {
        auto srcName = root.leaf();
        writeStringAt(out, srcName, x + 1, y);
        x += srcName.size() + 2;
        y++;

        auto jpos = baseX + (x - baseX) / 2;
        return {x, y, jpos};
    }
}
//---------------------------------------------------------------------------
bool QueryPlan::runPipeline(const PlanPipeline& pipeline, double cardinalityEstimate) {
    // Build up the pipeline
    auto& scanInput = *inputs[pipeline.scanInput];

    struct SourceInfo {
        /// Position in the pipeline of the operator containing source
        unsigned op;
        /// Offset within the operator
        unsigned offset;

        bool operator<(const SourceInfo& other) const {
            return std::make_tuple(op, offset) < std::make_tuple(other.op, other.offset);
        }
    };
    // Find the required eqs for scan
    BitSet scanRequiredEqs = computeRequiredEq({pipeline.scanInput});
    /// Sources for eqs
    std::unordered_map<unsigned, SourceInfo> sources;
    auto scanProduced = scanInput.producedEq & scanRequiredEqs;
    for (unsigned eq : scanProduced)
        sources[eq] = {0, scanProduced.getIndex(eq)};

    // Find the required eqs
    BitSet requiredEqs = computeRequiredEq(pipeline.rels);
    auto newInput = makeUnique<Input>();
    newInput->producedEq = requiredEqs;
    newInput->sourceScan = &scanInput;

    uint64_t zeroColumnValue = ~0ull;
    unsigned zeroColumnPos = scanRequiredEqs.size();
    // Compute the probes
    SmallVec<const Hashtable*> probeTables;
    probeTables.reserve(pipeline.probes.size());
    SmallVec<unsigned> probeOps;
    probeOps.reserve(pipeline.probes.size());
    SmallVec<unsigned> probeOffsets;
    probeOffsets.reserve(pipeline.probes.size());
    newInput->sourceProbes.reserve(pipeline.probes.size());
    for (unsigned ind = 0; ind < pipeline.probes.size(); ind++) {
        auto& probe = pipeline.probes[ind];
        Input& input = *inputs[probe.probeInput];
        assert(!input.isBase());
        assert(input.ht);
        probeTables.push_back(input.ht.get());
        newInput->sourceProbes.push_back(&input);
        // We restrict cross products to be left deep to ensure only one probe per pipeline
        if (input.isCrossProduct) {
            // The key will come from the last column of scan
            assert(zeroColumnValue == ~0ull);
            if (input.keyEq == crossProductEq) {
                zeroColumnValue = 0;
            } else {
                zeroColumnValue = eqConstants.at(input.keyEq);
            }
            newInput->keyEq = input.keyEq;
            probeOps.push_back(0);
            probeOffsets.push_back(zeroColumnPos);
        } else {
            assert(sources.find(probe.probeKeyEq) != sources.end());
            auto src = sources.at(probe.probeKeyEq);
            probeOps.push_back(src.op);
            probeOffsets.push_back(src.offset);
        }

        for (auto& [eq, off] : input.eqOffsets) {
            if (sources.find(eq) != sources.end())
                continue;
            sources[eq] = {ind + 1, off};
        }
    }

    SmallVec<unsigned> outputOps;
    SmallVec<unsigned> outputOffsets;

    // The key must be in the first position
    SmallVec<std::pair<SourceInfo, unsigned>> outputSources;
    outputSources.reserve(requiredEqs.size() + 1);
    if (!pipeline.isOutput()) {
        newInput->ht = makeUnique<Hashtable>();
        newInput->htBuild = makeUnique<HashtableBuild>(*newInput->ht, cardinalityEstimate);
        if (pipeline.keyEq == crossProductEq) {
            // The key will come from the last column of scan
            if (zeroColumnValue == ~0ull) {
                zeroColumnValue = 0;
                newInput->keyEq = pipeline.keyEq;
            }
            newInput->isCrossProduct = true;
            newInput->htBuild->isCrossProduct = true;
            outputSources.emplace_back(SourceInfo{0, zeroColumnPos}, crossProductEq);
        } else {
            newInput->keyEq = pipeline.keyEq;
            assert(requiredEqs.contains(pipeline.keyEq));
            assert(sources.find(pipeline.keyEq) != sources.end());
            outputSources.emplace_back(sources.at(pipeline.keyEq), pipeline.keyEq);
        }
    }

    // We need to sort the rest based on operator
    // This helps us compile fewer pipelines
    for (unsigned eq : requiredEqs) {
        if (eq == pipeline.keyEq)
            continue;
        assert(sources.find(eq) != sources.end());
        outputSources.emplace_back(sources.at(eq), eq);
    }
    std::sort(outputSources.begin() + (pipeline.isOutput() ? 0 : 1), outputSources.end());

    // Actually fill up the tables
    for (auto& [src, eq] : outputSources) {
        newInput->eqOffsets[eq] = outputOps.size();
        outputOps.push_back(src.op);
        outputOffsets.push_back(src.offset);
    }

    double mult = 1.0;
    for (auto* ht : probeTables)
        mult *= double(ht->getNumTuples()) / ht->getNumKeysEstimate();

    TableScan scan = buildScan(scanInput, scanRequiredEqs, mult);
    if (zeroColumnValue != ~0ull)
        scan.produceConstantColumn = zeroColumnValue;
    // Optional table target if this is the last pipeline
    UniquePtr<TableTarget> tableTarget;
    TargetBase* target;
    if (pipeline.isOutput()) {
        SmallVec<DataType> types;
        for (auto& [s, eq] : outputSources)
            types.push_back(attributes[equivalenceSets[eq].front()].dataType);
        tableTarget = makeUnique<TableTarget>(std::move(types));
        target = tableTarget.get();
    } else {
        newInput->ht->pretty = scan.getTableName();
        target = newInput->htBuild.get();
    }

    char pipelineNameBuffer[1024];
    size_t offset = 0;
#define PRINT(...) offset += std::snprintf(pipelineNameBuffer + offset, sizeof(pipelineNameBuffer) - offset, __VA_ARGS__)
    // Write target name, scan name, and number of probes
    auto targetName = target->getName();
    auto scanName = scan.getName();

    // Write target name, scan name, and number of probes
    PRINT("%.*s,%.*s,%zu,(", static_cast<int>(targetName.size()), targetName.data(), static_cast<int>(scanName.size()), scanName.data(), pipeline.probes.size());

    // Append probeOps
    for (unsigned op : probeOps)
        PRINT("%u", op);

    // Separator between probeSources and attrSources
    PRINT("),(");

    // Append outputOps
    for (unsigned op : outputOps)
        PRINT("%u", op);

    // Final closing parenthesis
    PRINT(")");
#undef PRINT

    std::string_view pipelineName{pipelineNameBuffer, offset};

    PipelineFunction pipelineFunction = PipelineFunctions::lookupPipeline(pipelineName);
    // Run the pipeline
    pipelineFunction(*target, scan, probeTables, probeOffsets, outputOffsets);

    if (pipeline.isOutput()) {
        SmallVec<std::variant<unsigned, RuntimeValue>> outputValues;
        for (unsigned eq : outputEqs) {
            if (auto it = eqConstants.find(eq); it != eqConstants.end()) {
                auto type = attributes[equivalenceSets[eq].front()].dataType;
                outputValues.emplace_back(RuntimeValue::from(type, it->second));
            } else {
                auto iter = std::find_if(outputSources.begin(), outputSources.end(), [&](auto& p) { return p.second == eq; });
                assert(iter != outputSources.end());
                unsigned ind = iter - outputSources.begin();
                outputValues.push_back(ind);
            }
        }
        assert(tableTarget);
        finalResult = tableTarget->prepareAndExtract(outputValues);
        return true;
    }
    assert(!pipeline.isOutput());
    if (newInput->ht->getNumTuples() == 0) {
        inputs.clear();
        return false;
    }
    bool singleton = newInput->ht->getNumTuples() == 1;
    // Do not use singleton to simplify for now
    // TODO: cries... Why is this so complicated?
    // We just need to be producing a single value that others also have, and the ht needs to be duplicate free
    bool simplified = (requiredEqs.single() && equivalenceSets[requiredEqs.front()].size() > 1 && newInput->ht->isDuplicateFree());
    if (simplified) {
        for (size_t i = 0; i < inputs.size(); i++) {
            if (pipeline.rels.contains(i))
                continue;

            // We currently do not want to filter existing hash tables, so we need to run the join
            if (!inputs[i]->isBase() && inputs[i]->producedEq.contains(requiredEqs.front())) {
                simplified = false;
                break;
            }
        }
    }

    Restriction rest{simplified ? Restriction::JoinPrecise : Restriction::Join, {}, newInput->ht.get()};
    newInput->restrictionLogic = RestrictionLogic::setupRestriction(rest);
    assert(newInput->restrictionLogic);
    auto* newRestriction = newInput->restrictionLogic.get();
    eqRestrictions[pipeline.keyEq] = newRestriction;
    newInput->cardinality = double(newInput->ht->getNumTuples());
    auto* newInputPtr = newInput.get();

    SmallVec<UniquePtr<Input>> newInputs;
    for (size_t i = 0; i < inputs.size(); i++) {
        // Are we removing this input?
        if (pipeline.rels.contains(i)) {
            graveyard.push_back(std::move(inputs[i]));
        } else {
            newInputs.push_back(std::move(inputs[i]));
        }
    }
    if (singleton) {
        uint64_t* tuple = nullptr;
        newInputPtr->ht->iterateAll([&](uint64_t& key) {
            assert(!tuple);
            tuple = &key;
        });
        assert(tuple);
        for (auto& [eq, off] : newInputPtr->eqOffsets) {
            if (eq == crossProductEq)
                continue;
            uint32_t value = tuple[off];
            if ((equivalenceSets[eq].size()) > 1 && (eq != pipeline.keyEq)) {
                Restriction rest{Restriction::Eq, RuntimeValue::from(attributes[equivalenceSets[eq].front()].dataType, value)};
                auto& logic = newInputPtr->additionalRestrictionLogics.emplace_back(RestrictionLogic::setupRestriction(rest));
                eqRestrictions[eq] = logic.get();
            }
            for (auto& input : newInputs) {
                if (!input->producedEq.contains(eq))
                    continue;
                // Not easy to handle the hash table case
                if (!input->isBase())
                    continue;
                input->sampleMatches = newRestriction->run(input->sample.data() + input->sampleOffsets.at(eq), input->sampleMatches);
                input->recomputeCardinality();
            }
        }
    }
    if (!simplified)
        newInputs.push_back(std::move(newInput));
    else
        graveyard.push_back(std::move(newInput));
    inputs = std::move(newInputs);

    for (auto& input : inputs) {
        if (input->isBase() && input->producedEq.contains(pipeline.keyEq)) {
            unsigned off = input->sampleOffsets.at(pipeline.keyEq);
            input->sampleMatches = newRestriction->run(input->sample.data() + off, input->sampleMatches);
            input->recomputeCardinality();
        }
    }
    return false;
}
//---------------------------------------------------------------------------
void QueryPlan::eliminateSingletons() {
    // Eliminate empty tables
    for (size_t i = 0; i < inputs.size(); i++) {
        assert(inputs[i]->isBase());
        if (inputs[i]->table->numRows > 1)
            continue;
        if (inputs[i]->table->numRows == 0) {
            inputs.clear();
            break;
        }
        assert(inputs[i]->table->numRows == 1);
        // Do not eliminate the last table
        if (inputs.size() == 1)
            break;
        TableScan scan = buildScan(*inputs[i], inputs[i]->producedEq, 1);
        CollectorTarget ct;
        ct.collect(scan);
        bool found = !ct.values.empty();
        if (!found) {
            inputs.clear();
            break;
        }
        for (unsigned eq : inputs[i]->producedEq) {
            auto val = ct.values[inputs[i]->producedEq.getIndex(eq)];
            eqConstants[eq] = val;
            if (equivalenceSets[eq].size() > 1) {
                Restriction rest{Restriction::Eq, RuntimeValue::from(attributes[equivalenceSets[eq].front()].dataType, val)};
                inputs[i]->restrictionLogic = RestrictionLogic::setupRestriction(rest);
                eqRestrictions[eq] = inputs[i]->restrictionLogic.get();
            }
        }

        graveyard.push_back(std::move(inputs[i]));
        inputs[i] = std::move(inputs.back());
        inputs.pop_back();
        i--;
    }
}
//---------------------------------------------------------------------------
void QueryPlan::computeSamples() {
    // Compute samples
    Scheduler::parallelFor(0, inputs.size(), [&](size_t workerId, size_t inputInd) {
        auto& input = inputs[inputInd];
        assert(input->isBase());
        BitSet intEqs;
        for (unsigned attr : input->producedAttributes) {
            unsigned eq = attributes[attr].eqClass;
            if ((attributes[attr].dataType == DataType::INT32) && (equivalenceSets[eq].size() > 1)) {
                intEqs.insert(eq);
                input->sampleOffsets[eq] = 0;
            }
        }
        input->sampleSize = std::min(input->table->numRows, size_t(64));
        for (auto& [eq, off] : input->sampleOffsets) {
            input->sampleOffsets[eq] = input->sampleSize * intEqs.getIndex(eq);
        }
        auto scan = buildScan(*input, intEqs, 1);
        input->sample = scan.createUnfilteredSample(input->sampleSize);
        input->sampleMatches = (~0ull >> (64 - input->sampleSize));
        for (auto& [eq, off] : input->sampleOffsets) {
            assert(eqRestrictions.find(eq) != eqRestrictions.end());
            auto* rest = eqRestrictions.at(eq);
            input->sampleMatches = rest->run(input->sample.data() + off, input->sampleMatches);
        }
        input->recomputeCardinality();
    });
}
//---------------------------------------------------------------------------
ColumnarTable QueryPlan::run() {
    for (unsigned eq = 0; eq < equivalenceSets.size(); eq++) {
        assert(!equivalenceSets[eq].empty());
        if (!equivalenceSets[eq].single())
            eqRestrictions[eq] = RestrictionLogic::notNullRestriction;
    };

    eliminateSingletons();
    computeSamples();

    while (!inputs.empty()) {
        // Optimize the join plan
        SmallVec<QueryGraph::Input> qgInputs;
        qgInputs.reserve(inputs.size());
        BitSet constants;
        for (auto& [k, v] : eqConstants)
            constants.insert(k);
        for (auto& input : inputs) {
            assert(input->isBase() == (input->keyEq == ~0u));
            double mult = 1.0;
            if (!input->isBase())
                mult = double(input->ht->getNumTuples()) / input->ht->getNumKeysEstimate();
            qgInputs.push_back({input->producedEq - constants, input->cardinality, mult, input->keyEq});
        }
        QueryGraph qg(*this, qgInputs);
        QueryGraph::Plan* root = qg.optimize();

        // Find the cheapest pipeline within the join plan
        PlanPipeline pipeline = CheapestPipelineFinder::findCheapestPipeline(qg, root);
        assert(!!pipeline);
        assert(!pipeline.rels.empty());

        if (runPipeline(pipeline, root->card))
            return std::move(finalResult);
    }

    // Empty input
    SmallVec<DataType> outputTypes;
    for (unsigned eq : outputEqs)
        outputTypes.push_back(attributes[equivalenceSets[eq].front()].dataType);
    TableTarget tableTarget(std::move(outputTypes));
    TableTarget::LocalState ls(tableTarget);
    tableTarget.finishConsume();
    auto res = tableTarget.extract();
    tableTarget.localStates.clear();
    return res;
}
//---------------------------------------------------------------------------
}
