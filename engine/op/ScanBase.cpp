#include "op/ScanBase.hpp"
#include "infra/Scheduler.hpp"
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
size_t ScanBase::concurrency() const {
    return Scheduler::concurrency();
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------