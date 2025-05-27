#include "op/CollectorTarget.hpp"
#include "pipeline/PipelineGen.hpp"
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
CollectorTarget::LocalState::LocalState(CollectorTarget& target) {
    next = target.localStates.exchange(this);
}
//---------------------------------------------------------------------------
void CollectorTarget::finishConsume() {
    for (auto* ls = localStates.load(); ls; ls = ls->next)
        values.insert(values.end(), ls->values.begin(), ls->values.end());
}
//---------------------------------------------------------------------------
template <size_t... Is>
static auto collectCallback(CollectorTarget& collector, engine::TableScan& op, SmallVec<unsigned, 8>& attrOffsets, std::index_sequence<Is...>) {
    PipelineFunctions::runPipeline<CollectorTarget, TableScan, 0, std::index_sequence<>, std::index_sequence<Is - Is...>>(collector, op, {}, {}, {attrOffsets.data(), attrOffsets.size()});
}
//---------------------------------------------------------------------------
void CollectorTarget::collect(engine::TableScan& op) {
    size_t cols = op.getProducedColumns();
    SmallVec<unsigned, 8> attrOffsets;
    for (size_t i = 0; i < cols; i++)
        attrOffsets.push_back(i);

    switch (cols) {
        case 1: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<1>{});
        case 2: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<2>{});
        case 3: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<3>{});
        case 4: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<4>{});
        case 5: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<5>{});
        case 6: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<6>{});
        case 7: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<7>{});
        case 8: return collectCallback(*this, op, attrOffsets, std::make_index_sequence<8>{});
        default:
            throw std::runtime_error("Unsupported number of columns");
    }
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
