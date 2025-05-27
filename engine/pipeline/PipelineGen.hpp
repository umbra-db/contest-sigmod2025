#pragma once
//---------------------------------------------------------------------------
#include "op/Hashtable.hpp"
#include "op/TableScan.hpp"
#include "op/TableTarget.hpp"
#include "pipeline/JoinPipeline.hpp"
#include "pipeline/PipelineFunction.hpp"
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template<size_t ... Is>
auto genProbes(engine::span<const DefaultProbeParameter>& probeParams, std::index_sequence<Is...>) {
    return std::tuple{DefaultProbe{probeParams[Is]}...};
}
//---------------------------------------------------------------------------
template <typename Target, typename Scan, size_t NumJoins, typename Keys, typename Attrs>
void PipelineFunctions::runPipeline(TargetBase& targetBase, ScanBase& scanBase, engine::span<const DefaultProbeParameter> probeParams, engine::span<const unsigned> keyOffsets, engine::span<const unsigned> attrOffsets) {
    static_assert(std::is_base_of_v<TargetBase, Target>);
    auto& target = dynamic_cast<Target&>(targetBase);
    auto& scan = dynamic_cast<Scan&>(scanBase);
    assert(keyOffsets.size() == Keys::size());
    assert(attrOffsets.size() == Attrs::size());
    std::array<unsigned, Keys::size()> ko;
    for (size_t i = 0; i < Keys::size(); ++i)
        ko[i] = keyOffsets[i];
    std::array<unsigned, Attrs::size()> ao;
    for (size_t i = 0; i < Attrs::size(); ++i)
        ao[i] = attrOffsets[i];
    auto probes = genProbes(probeParams, std::make_index_sequence<NumJoins>{});
    JoinPipeline<Target, Scan, decltype(probes), Keys, Attrs> pipeline{target, scan, probes, ko, ao};
    pipeline();
}
//---------------------------------------------------------------------------
}
