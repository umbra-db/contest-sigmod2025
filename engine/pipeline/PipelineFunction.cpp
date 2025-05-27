#include "pipeline/PipelineFunction.hpp"
#include <algorithm>
#include <stdexcept>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
PipelineFunction PipelineFunctions::lookupPipeline(std::string_view name) {
    // Binary search in functions
    auto it = std::lower_bound(functions, functions + numFunctions, name, [](const auto& f, std::string_view name) {
        return f.first < name;
    });
    if (it == functions + numFunctions || it->first != name)
        throw std::runtime_error("Pipeline not found");
    return it->second;
}
//---------------------------------------------------------------------------
}
