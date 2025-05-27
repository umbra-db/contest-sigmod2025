#pragma once
//---------------------------------------------------------------------------
#include "Config.hpp"
#include "infra/SmallVec.hpp"
#include "infra/Util.hpp"
#include "infra/QueryMemory.hpp"
#include "op/TargetBase.hpp"
#include "query/DataSource.hpp"
#include "query/RuntimeValue.hpp"

#include <atomic>
#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <variant>
#include <attribute.h>
//---------------------------------------------------------------------------
struct ColumnarTable;
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
namespace impl {
class Writer;
}
//---------------------------------------------------------------------------
class TableTarget : public TargetImpl<TableTarget> {
    public:
    struct LocalState {
        /// Number of tuples in a buffer
        static constexpr size_t bufferCount = 64;
        /// Current position in the buffer
        uint64_t* curBuffer;
        /// End position in the buffer
        uint64_t* endBuffer;
        /// num rows
        size_t numRows = 0;
        /// The column writers
        SmallVec<UniquePtr<impl::Writer>> writers;
        /// The output buffer
        uint64_t* buffer;
        /// indicating whether the current batch has nulls
        /// one bit per column
        uint32_t hasNulls = 0;
        /// The next state
        LocalState* next = nullptr;

        LocalState(TableTarget& target);
        ~LocalState() noexcept;
        LocalState(LocalState&&) noexcept;
        LocalState& operator=(LocalState&&) noexcept;

        void flushBuffers();
    };
    std::atomic<LocalState*> localStateRefs = nullptr;
    Vector<LocalState*> localStates;
    /// The data types
    SmallVec<DataType> types;

    /// Make a writer given type
    static UniquePtr<impl::Writer> makeWriter(DataType type);

    /// Constructor
    explicit TableTarget(SmallVec<DataType>&& types);
    /// Destructor
    ~TableTarget() noexcept;

    /// Consume attributes
    template <typename... AttrT>
    void operator()(LocalState& ls, uint64_t multiplicity, AttrT... attrs) {
        const auto& writers = ls.writers;
        // The first attr is multiplicity
        assert(sizeof...(attrs) == writers.size());
        unsigned ind = 0;
        auto* cur = ls.curBuffer;
        if constexpr (config::handleMultiplicity) {
            (*cur = multiplicity, cur += LocalState::bufferCount);
        }
        ((*cur = attrs, cur += LocalState::bufferCount), ...);
        ind = 0;
        ((ls.hasNulls |= (attrs == RuntimeValue::nullValue) << (ind++)), ...);
        ls.curBuffer++;
        if (ls.curBuffer == ls.endBuffer)
            ls.flushBuffers();
    }

    /// Flush collected
    void finishConsume();
    /// Flush within current local state
    void finalize(LocalState& ls) const;

    /// Prepare the output.
    /// An output column is either a column that has been collected by table target or a constant.
    /// For a constant a column of just that constant needs to be constructed.
    ColumnarTable prepareAndExtract(const SmallVec<std::variant<unsigned, RuntimeValue>>& columns);
    /// Extract the result
    ColumnarTable extract();

    std::string getPretty() const override;
};
//---------------------------------------------------------------------------
static_assert(TargetOperator<TableTarget, 1>);
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
