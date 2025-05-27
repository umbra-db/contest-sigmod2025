#pragma once
//---------------------------------------------------------------------------
#include "infra/Reflection.hpp"
#include "op/OpBase.hpp"
#include "pipeline/PipelineConcepts.hpp"
#include <cassert>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// Base class for all targets
class TargetBase : public OpBase {
    public:
    /// Get the target type
    virtual std::string_view getName() const = 0;
};
//---------------------------------------------------------------------------
/// All targets must inherit from target impl
template <typename T>
class TargetImpl : public TargetBase {
    public:
    /// Return the name of the class
    std::string_view getName() const override {
        static_assert(TargetOperator<T, 1>, "T must be a proper target");
        return ClassInfo::getName<T>();
    }
};
//---------------------------------------------------------------------------
}
