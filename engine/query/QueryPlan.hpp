#pragma once
//---------------------------------------------------------------------------
#include "infra/BitSet.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/SmallVec.hpp"
#include "query/DataSource.hpp"
#include "query/Restriction.hpp"
#include "query/RuntimeValue.hpp"
#include <variant>
#include <vector>
#include <attribute.h>
#include <plan.h>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class TableTarget;
class Hashtable;
class HashtableBuild;
class TableScan;
class RestrictionLogic;
class QueryGraph;
//---------------------------------------------------------------------------
class QueryPlan {
    public:
    /// An attribute corresponding to a physical column
    struct Attribute;
    /// An input. It is either a base table or a hash table built on an attribute
    struct Input;

    private:
    /// The query graph for optimizing joins
    friend class QueryGraph;
    /// A pipeline descriptor
    struct PlanPipeline;
    struct CheapestPipelineFinder;
    /// The input data
    DataSource* db;
    /// All attributes provided by table scans in the query plan
    Vector<Attribute> attributes;
    /// The output equivalence classes
    SmallVec<unsigned> outputEqs;
    /// The inputs
    SmallVec<UniquePtr<Input>> inputs;
    /// The used inputs that we have to keep alive
    SmallVec<UniquePtr<Input>> graveyard;
    /// Sets of attributes within each equivalence class
    SmallVec<BitSet> equivalenceSets;
    /// Equivalence classes for which we have a constant value
    UnorderedMap<unsigned, uint64_t> eqConstants;
    /// Equivalence classes for which we have a restriction
    UnorderedMap<unsigned, const RestrictionLogic*> eqRestrictions;
    /// The result
    ColumnarTable finalResult;

    /// Estimate the cardinality of a table and the selectivities of its restrictions
    void estimateCardinality(Input& input);
    /// Build a table scan for an input
    TableScan buildScan(Input& input, BitSet requiredEqs, double mult);
    /// Compute the required equivalence classes outside of a set of relations
    BitSet computeRequiredEq(BitSet relations);
    /// Eliminate singletons
    void eliminateSingletons();
    /// Compute samples
    void computeSamples();
    /// Run a pipeline
    bool runPipeline(const PlanPipeline& pipeline, double cardinalityEstimate);
    /// Print a query plan
    void printPlan(Input& root) const;

    public:
    /// Constructor
    explicit QueryPlan(DataSource& db);
    /// Destructor
    ~QueryPlan() noexcept;
    /// Move constructor
    QueryPlan(QueryPlan&&) noexcept;
    /// Move assignment
    QueryPlan& operator=(QueryPlan&&) noexcept;

    /// Add an input
    void addInput(DataSource::Table& table, BitSet attrs);
    /// Add an attribute
    void addAttribute(unsigned relation, unsigned column, DataType dataType);
    /// Prepare query plan after all inputs and attributes have been added
    void prepare(SmallVec<BitSet> equivalenceSets);
    /// Set the output attribtues
    void setOutput(engine::span<const unsigned> attrs);

    /// Run the query
    ColumnarTable run();
};
//---------------------------------------------------------------------------
}
