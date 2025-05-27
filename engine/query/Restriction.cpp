#include "query/Restriction.hpp"
#include "op/Hashtable.hpp"
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
bool Restriction::operator()(uint64_t val) const noexcept {
    switch (type) {
        case Eq:
            assert(cst.value != nullValue);
            return val == cst.value;
        case NotNull: return val != nullValue;
        case Join: return (val != nullValue) && joinFilter->joinFilter(val);
        case JoinPrecise: return (val != nullValue) && joinFilter->joinFilterPrecise(val);
    }
    __builtin_unreachable();
}
//---------------------------------------------------------------------------
}