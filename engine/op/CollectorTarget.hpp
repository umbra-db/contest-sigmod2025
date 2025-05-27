#pragma once
//---------------------------------------------------------------------------
#include "infra/SmallVec.hpp"
#include "op/TargetBase.hpp"
#include <atomic>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class TableScan;
//---------------------------------------------------------------------------
/// A very simple collector target
class CollectorTarget : public TargetImpl<CollectorTarget> {
    public:
    struct LocalState {
        /// The values
        SmallVec<uint64_t> values;
        /// The next local state
        LocalState* next = nullptr;

        LocalState(CollectorTarget& target);
    };
    /// The local states
    std::atomic<LocalState*> localStates = nullptr;
    /// The values
    SmallVec<uint64_t> values;

    /// Consume attributes
    template <typename... AttrT>
    void operator()(LocalState& ls, uint64_t multiplicity, AttrT... attrs) {
        for (uint64_t i = 0; i < multiplicity; i++)
            (ls.values.push_back(attrs), ...);
    }

    /// Flush collected
    void finishConsume();

    /// Collect the output from a table scan
    void collect(TableScan& op);
};
//---------------------------------------------------------------------------
static_assert(TargetOperator<CollectorTarget, 1>);
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
