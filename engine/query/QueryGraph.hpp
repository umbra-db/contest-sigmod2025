#pragma once
//---------------------------------------------------------------------------
#include "infra/BitSet.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/SmallVec.hpp"
#include <vector>
#include <map>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class QueryPlan;
//---------------------------------------------------------------------------
struct QueryGraph {
    struct Input {
        /// The equivalence classes being produced
        BitSet producedEq;
        /// The cardinality estimation
        double cardinality = 1.0;
        /// The multiplicity for hash tables
        double multiplicity = 1.0;
        /// The key that needs to be joined with
        unsigned joinKey = ~0u;
    };
    struct Plan {
        Plan* left = nullptr;
        Plan* right = nullptr;
        BitSet set;
        size_t pipes;
        BitSet eqs;
        mutable BitSet neighborhood;
        double card = -1;
        double bc = 0.0;
        double mc = 1.0;
        double cost = std::numeric_limits<double>::infinity();

        bool isLeaf() const { return !left; }
    };

    /// The maximum number of pipes in a plan
    static constexpr size_t maxPipelineLength = 3;

    /// Reference to the query plan
    QueryPlan& qp;
    /// The inputs
    SmallVec<Input> inputs;
    /// The plans
    Vector<std::array<Plan, maxPipelineLength>> plans;
    /// The number of pipes of the best plan
    size_t bestPipes = 0;

    explicit QueryGraph(QueryPlan& qp, SmallVec<Input> inputs);

    size_t size() const;

    BitSet computeNeighborhood(BitSet rels, BitSet eqs) const;
    BitSet neighborhood(BitSet bs) const;
    bool connected(BitSet bs) const;
    bool canJoin(BitSet left, BitSet right);
    void consider(BitSet left, BitSet right);
    static double computeCost(double card, double leftCard, [[maybe_unused]] double rightCard);

    /// Get a plan
    [[gnu::always_inline]] Plan& get(BitSet bs, size_t pipes) {
        assert(pipes < maxPipelineLength);
        assert(bs.asU64() < plans.size());
        auto& result = plans[bs.asU64()][pipes];
        assert(result.set == bs);
        assert(result.pipes == pipes);
        return result;
    }

    /// Compute cardinality
    double computeCard(BitSet rels);
    /// Compute cardinality
    void computeCard(Plan& target, const Plan& left, const Plan& right);

    Plan* optimize();
};
//---------------------------------------------------------------------------
}
