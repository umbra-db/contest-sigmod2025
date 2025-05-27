#pragma once
//---------------------------------------------------------------------------
#include "infra/QueryMemory.hpp"
#include "storage/BitLogic.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct Restriction;
//---------------------------------------------------------------------------
class RestrictionLogic {
    public:
    /// Filter masked values and return a bitset mask
    uint64_t run(const uint32_t* values, uint64_t mask) const {
        if (!mask)
            return 0;
        if (BitLogic::isDense(mask)) [[likely]] {
            auto [st, en] = BitLogic::getRange(mask);
            auto len = en - st;
            auto result = runDense(values + st, len);
            return result << st;
        } else {
            return runSparse(values, mask);
        }
    }
    virtual uint64_t runSparse(const uint32_t* values, uint64_t mask) const = 0;
    virtual uint64_t runDense(const uint32_t* values, size_t len) const = 0;
    virtual std::pair<uint64_t, size_t> runAndSkip(const uint32_t* values, size_t len) const = 0;
    /// Estimate the selectivity very broadly
    virtual double estimateSelectivity() const = 0;
    virtual double estimateCost() const = 0;
    /// Destructor
    virtual ~RestrictionLogic() noexcept = default;

    /// The null restriction
    static const RestrictionLogic* notNullRestriction;

    /// Setup a restriction logic given restriction
    static UniquePtr<RestrictionLogic> setupRestriction(const Restriction& restriction);

    virtual std::string_view name() const = 0;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
