#pragma once
//---------------------------------------------------------------------------
#include "pipeline/PipelineConcepts.hpp"
#include "op/OpBase.hpp"
#include "infra/Reflection.hpp"
#include <string_view>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// Base class for all scans
class ScanBase : public OpBase {
    public:
    /// Get the target type
    virtual std::string_view getName() const = 0;
    /// Get the concurrency
    virtual size_t concurrency() const;
};
//---------------------------------------------------------------------------
/// All targets must inherit from target impl
template <typename T>
class ScanImpl : public ScanBase {
    public:
    /// Return the name of the class
    std::string_view getName() const override {
        static_assert(ScanOperator<T>, "T must be a proper scan");
        return ClassInfo::getName<T>();
    }
};
//---------------------------------------------------------------------------
}
