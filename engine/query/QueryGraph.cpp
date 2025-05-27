#include "query/QueryGraph.hpp"
#include "query/DPccp.hpp"
#include "query/QueryPlan.hpp"
#include <cmath>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
QueryGraph::QueryGraph(QueryPlan& qp, SmallVec<Input> inputs) : qp(qp), inputs(std::move(inputs)), plans(1ull << qp.inputs.size()) {
    for (size_t i = 0; i < plans.size(); i++) {
        for (size_t p = 0; p < maxPipelineLength; p++) {
            plans[i][p].set = BitSet::fromU64(i);
            plans[i][p].pipes = p;
        }
    }
}
//---------------------------------------------------------------------------
size_t QueryGraph::size() const { return qp.inputs.size(); }
//---------------------------------------------------------------------------
BitSet QueryGraph::computeNeighborhood(BitSet rels, BitSet eqs) const {
    BitSet result;
    for (unsigned i = 0; i < qp.inputs.size(); i++)
        if (eqs.intersectsWith(inputs[i].producedEq))
            result.insert(i);
    return result - rels;
}
//---------------------------------------------------------------------------
BitSet QueryGraph::neighborhood(BitSet bs) const {
    if (plans[bs.asU64()][0].eqs.empty()) {
        BitSet eqs;
        for (unsigned r : bs)
            eqs += inputs[r].producedEq;
        return computeNeighborhood(bs, eqs);
    }
    return plans[bs.asU64()][0].neighborhood;
}
//---------------------------------------------------------------------------
bool QueryGraph::connected(BitSet bs) const {
    if (plans[bs.asU64()][0].card == -1)
        return false;
    for (size_t i = 0; i < size(); i++)
        if (plans[bs.asU64()][i].cost < std::numeric_limits<double>::infinity())
            return true;
    return false;
}
//---------------------------------------------------------------------------
double QueryGraph::computeCost(double card, double leftCard, [[maybe_unused]] double rightCard) {
    auto res = card + leftCard * 10;
    assert(std::isfinite(res));
    return res;
}
//---------------------------------------------------------------------------
/// Compute cardinality
double QueryGraph::computeCard(BitSet rels) {
    // Return the maximum over all relations
    double card = 0;
    double mult = 1;
    for (unsigned u : rels) {
        card = std::max(card, inputs[u].cardinality);
    }
    assert(std::isfinite(card * mult));
    return card * mult;
}
//---------------------------------------------------------------------------
/// Compute cardinality
void QueryGraph::computeCard(Plan& target, const Plan& left, const Plan& right) {
    target.bc = std::max(left.bc, right.bc);
    target.card = target.bc;
}
//---------------------------------------------------------------------------
void QueryGraph::consider(BitSet left, BitSet right) {
    auto tot = left + right;
    double baseCost = computeCost(get(tot, 0).card, get(left, 0).card, get(right, 0).card);
    for (size_t rpipes = 0; rpipes < maxPipelineLength - 1; rpipes++) {
        auto& rplan = get(right, rpipes);
        auto& target = get(tot, rpipes + 1);
        if (rplan.cost >= target.cost)
            continue;
        double bound = target.cost - (rplan.cost + baseCost);
        for (size_t lpipes = 0; lpipes < maxPipelineLength; lpipes++) {
            auto& lplan = get(left, lpipes);
            if (lplan.cost < bound) {
                double cost = lplan.cost + (rplan.cost + baseCost);
                assert(std::isfinite(cost));
                target.cost = cost;
                target.left = &lplan;
                target.right = &rplan;
            }
        }
    }
}
//---------------------------------------------------------------------------
bool QueryGraph::canJoin(BitSet left, BitSet right) {
    auto leftp = get(left, 0);
    auto rightp = get(right, 0);
    auto eqIntersection = leftp.eqs & rightp.eqs;
    if (eqIntersection.empty())
        return false;
    /// The right side cannot be a hash table
    if (right.singleNonEmpty() && inputs[right.front()].joinKey != ~0u)
        return false;
    // If left side is a hash table, we need to respect its key
    if (left.singleNonEmpty() && inputs[left.front()].joinKey != ~0u)
        return eqIntersection.contains(inputs[left.front()].joinKey);
    return true;
}
//---------------------------------------------------------------------------
QueryGraph::Plan* QueryGraph::optimize() {
    // Setup base relations
    for (unsigned i = 0; i < qp.inputs.size(); i++) {
        auto& p = plans[BitSet{i}.asU64()][0];
        p.eqs = inputs[i].producedEq;
        p.neighborhood = computeNeighborhood(p.set, p.eqs);
        p.bc = inputs[i].cardinality;
        p.card = inputs[i].cardinality;
        // Not sure we can do any kind of view matching but just to be safe, give cost to base relations
        p.cost = p.card;
    }

    // Compute optimal plan
    DPccp::enumerateCsgCmp(*this, [&](BitSet left, BitSet right) -> void {
        assert(get(left, 0).card >= 0);
        assert(get(left, 0).card < std::numeric_limits<double>::infinity());
        assert(get(right, 0).card >= 0);
        assert(get(right, 0).card < std::numeric_limits<double>::infinity());
        auto tot = left + right;

        auto& totBase = get(tot, 0);
        if (totBase.card == -1) {
            totBase.eqs = get(left, 0).eqs + get(right, 0).eqs;
            computeCard(totBase, get(left, 0), get(right, 0));
            totBase.neighborhood = computeNeighborhood(tot, totBase.eqs);
            for (size_t i = 1; i < maxPipelineLength; i++) {
                auto& totPlan = get(tot, i);
                totPlan.eqs = totBase.eqs;
                totPlan.card = totBase.card;
                totPlan.neighborhood = totBase.neighborhood;
            }
        }
        bool lr = canJoin(left, right);
        bool rl = canJoin(right, left);
        if (lr)
            consider(left, right);
        if (rl)
            consider(right, left);
    });

    auto full = BitSet::prefix(qp.inputs.size());

    for (size_t i = 0; i < maxPipelineLength; i++) {
        if (get(full, i).cost < get(full, bestPipes).cost)
            bestPipes = i;
    }

    // Handle cross products
    if (get(full, bestPipes).cost == std::numeric_limits<double>::infinity()) {
        SmallVec<BitSet> componentSets;
        BitSet remaining = full;
        while (!remaining.empty()) {
            if (connected(remaining)) {
                componentSets.push_back(remaining);
                break;
            }
            for (BitSet sub : remaining.subsets()) {
                // We should exit the loop before nothing is left
                assert(sub != remaining);
                auto set = remaining - sub;
                if (!connected(set))
                    continue;

                // We found the largest subset of remaining that is connected
                // This set may not intersect with any of the existing components
                assert(std::all_of(componentSets.begin(), componentSets.end(), [&](const BitSet& c) {
                    return !c.intersectsWith(set);
                }));
                componentSets.push_back(set);
                remaining -= set;
            }
        }

        std::sort(componentSets.begin(), componentSets.end(), [&](const BitSet& a, const BitSet& b) {
            // Both sides cannot be a hash table as we only allow one cross product hash table at a time
            assert(!(a.single() && inputs[a.front()].joinKey != ~0u) || !(b.single() && inputs[b.front()].joinKey != ~0u));
            // The cross product we built a hash table on must come first
            if (a.single() && inputs[a.front()].joinKey != ~0u)
                return true;
            if (b.single() && inputs[b.front()].joinKey != ~0u)
                return false;
            return get(a, 0).card < get(b, 0).card;
        });

        BitSet cur = componentSets[0];
        for (size_t i = 1; i < componentSets.size(); i++) {
            auto next = componentSets[i];
            BitSet tot = cur + next;
            assert(connected(cur));
            assert(connected(next));
            computeCard(get(tot, 0), get(cur, 0), get(next, 0));
            get(tot, 0).eqs = get(cur, 0).eqs + get(next, 0).eqs;
            for (size_t p = 1; p < maxPipelineLength; p++)
                get(tot, p).card = get(tot, 0).card;
            consider(cur, next);
            assert(connected(tot));
            cur = tot;
        }
        assert(cur == full);
    }

    for (size_t i = 0; i < maxPipelineLength; i++) {
        if (get(full, i).cost < get(full, bestPipes).cost)
            bestPipes = i;
    }

    assert(get(full, 0).card >= 0);
    assert(get(full, bestPipes).cost < std::numeric_limits<double>::infinity());
    return &get(full, bestPipes);
}
//---------------------------------------------------------------------------
}
