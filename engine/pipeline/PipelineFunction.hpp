#pragma once
//---------------------------------------------------------------------------
#include "pipeline/PipelineConcepts.hpp"
#include "infra/helper/Span.hpp"
#include "infra/Util.hpp"
#include "op/ScanBase.hpp"
#include "op/TargetBase.hpp"
#include <cassert>
#include <string_view>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class Hashtable;
class HashtableBuild;
class HashtableProbe;
class TableScan;
class TableTarget;
//---------------------------------------------------------------------------
using DefaultProbe = HashtableProbe;
//---------------------------------------------------------------------------
using DefaultProbeParameter = const Hashtable*;
//---------------------------------------------------------------------------
using PipelineFunction = void (*)(TargetBase& target, ScanBase& scan, engine::span<const DefaultProbeParameter> probes, engine::span<const unsigned> keyOffsets, engine::span<const unsigned> outputAttributeOffsets);
//---------------------------------------------------------------------------
class JoinPipelineBase {
    virtual ~JoinPipelineBase() noexcept = default;
};
//---------------------------------------------------------------------------
struct PipelineFunctions {
    static size_t numFunctions;
    static std::pair<std::string_view, PipelineFunction> functions[];

    static PipelineFunction lookupPipeline(std::string_view name);

    template <typename Target, typename Scan, size_t NumJoins, typename Keys, typename Attrs>
    static void runPipeline(TargetBase& target, ScanBase& scan, engine::span<const DefaultProbeParameter> probeParams, engine::span<const unsigned> keyOffsets, engine::span<const unsigned> attrOffsets);
};
//---------------------------------------------------------------------------
}
