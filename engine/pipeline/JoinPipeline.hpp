#pragma once
//---------------------------------------------------------------------------
#include "Config.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/Scheduler.hpp"
#include "infra/Util.hpp"
#include "pipeline/PipelineConcepts.hpp"
#include <cassert>
#include <cstddef>
#include <memory>
#include <tuple>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename Target, typename Scan, typename Probes, typename Keys, typename Attrs>
struct JoinPipeline;
//---------------------------------------------------------------------------
template <typename LocalState>
struct LocalStateContainer {
    size_t numWorkers;
    UniquePtr<std::byte> localStateRawData;
    std::byte* localStateRawDataPtr = nullptr;

    LocalStateContainer(size_t numWorkers) : numWorkers(numWorkers) {
        localStateRawData = UniquePtr<std::byte>(static_cast<std::byte*>(querymemory::allocate(numWorkers * sizeof(LocalState) + hardwareCachelineSize - 1)));
        localStateRawDataPtr = reinterpret_cast<std::byte*>((reinterpret_cast<uint64_t>(localStateRawData.get()) + hardwareCachelineSize - 1) & ~(hardwareCachelineSize - 1));
#ifndef NDEBUG
        for (size_t i = 0; i < numWorkers; i++) {
            reinterpret_cast<LocalState*>(&localStateRawDataPtr[i * sizeof(LocalState)])->initialized = false;
        }
#endif
    }

    LocalState* get(size_t workerId) {
        assert(workerId < numWorkers);
        return reinterpret_cast<LocalState*>(&localStateRawDataPtr[workerId * sizeof(LocalState)]);
    }
};
//---------------------------------------------------------------------------
/// A pipeline that probes multiple hash tables and then passes the result to a target
/// The 0th input relation is the Scan
/// The 1st input relation is the first probe, the 2nd input relation is the second probe, an so on
/// The Keys... parameter pack describes which input relations are used for the join keys
/// The Attrs... parameter pack describes which input relations are used for the output attributes
/// Each operator (scan and probes) give IU providers which are used to access the tuples attributes
template <typename Target, typename Scan, typename... Probes, size_t... Keys, size_t... Attrs>
struct JoinPipeline<Target, Scan, std::tuple<Probes...>, std::index_sequence<Keys...>, std::index_sequence<Attrs...>> {
    /// The probe operators
    std::tuple<Probes...> probes;
    /// The offsets of the key attributes within the relations
    std::array<unsigned, sizeof...(Keys)> keyOffsets;
    /// The offsets of the output attributes within the relations
    std::array<unsigned, sizeof...(Attrs)> attrOffsets;
    /// The local state structure
    struct alignas(hardwareCachelineSize) LocalState {
        bool initialized;
        size_t workerId;
        JoinPipeline* pipeline;
        typename Scan::LocalState scan;
        std::tuple<typename Probes::LocalState...> probes;
        typename Target::LocalState target;

        LocalState(size_t workerId, JoinPipeline* pipeline, Target& target, Scan& scan, Probes&... probes)
            : workerId(workerId), pipeline(pipeline), scan(scan), probes(typename Probes::LocalState(probes)...), target(target) {
        }
    };
    /// An object that has the shape of LocalState
    struct alignas(LocalState) LocalStateLike {
        std::array<std::byte, sizeof(LocalState)> data;
    };
    /// The local state container
    LocalStateContainer<LocalState> localStates;
    /// The target
    Target& target;
    /// The scan
    Scan& scan;

    /// Constructor
    JoinPipeline(Target& target, Scan& scan, decltype(probes) probes, decltype(keyOffsets) keyOffsets, decltype(attrOffsets) attrOffsets)
        : probes(probes), keyOffsets(keyOffsets), attrOffsets(attrOffsets), localStates(scan.concurrency() == 1 ? 1 : Scheduler::concurrency()), target(target), scan(scan) {
        static_assert(sizeof...(Probes) == sizeof...(Keys), "Number of probes must match number of keys");
        static_assert(((Keys <= sizeof...(Probes)) && ...), "All keys must be valid indices for the probes tuple");
        static_assert(((Attrs <= sizeof...(Probes)) && ...), "All attributes must be valid indices for the probes tuple");
        static_assert((ProbeOperator<Probes> && ...), "All probes must be ProbeOperators");
        static_assert(TargetOperator<Target, sizeof...(Attrs)>, "Target must be a TargetOperator with the correct number of attributes");
        static_assert(ScanOperator<Scan>, "Scan must be a ScanOperator");

        if constexpr (config::handleMultiplicity) {
            std::array<size_t, sizeof...(Keys)> keySourcesArray = {Keys...};
            // Probes have multiplicity as first attribute
            for (size_t i = 0; i < sizeof...(Keys); i++)
                this->keyOffsets[i] += keySourcesArray[i] != 0;
            std::array<size_t, sizeof...(Attrs)> attrSourcesArray = {Attrs...};
            // Probes have multiplicity as first attribute
            for (size_t i = 0; i < sizeof...(Attrs); i++)
                this->attrOffsets[i] += attrSourcesArray[i] != 0;
        }
    }

    /// Helper for getting the relation for the key of given index
    template <size_t Ind>
    static constexpr size_t getKey() { return std::get<Ind>(std::tuple{Keys...}); }

    /// Push the result to the target
    template <size_t... Is, typename... Providers, typename = std::enable_if_t<(Provider<Providers> && ...)>>
    [[gnu::always_inline]] void consumeTarget(LocalState& localState, Target& target, uint64_t multiplicity, std::index_sequence<Is...>, Providers... providers) {
        auto provs = std::forward_as_tuple(providers...);
        target(localState.target, multiplicity, std::get<Attrs>(provs)(attrOffsets[Is])...);
    }
    /// Handle the probe of given index
    template <size_t Ind = 0, typename... Providers, typename = std::enable_if_t<(Provider<Providers> && ...)>>
    [[gnu::always_inline]] void consumeProbe(LocalState& localState, Target& target, uint64_t multiplicity, Providers... providers) {
        if constexpr (Ind < sizeof...(Probes)) {
            auto key = std::get<getKey<Ind>()>(std::forward_as_tuple(providers...))(keyOffsets[Ind]);
            std::get<Ind>(probes)(std::get<Ind>(localState.probes), key, [&](auto provider) __attribute__((always_inline)) {
                auto mult = multiplicity;
                if constexpr (config::handleMultiplicity) {
                    if constexpr (Ind == 0) {
                        mult = provider(0);
                    } else {
                        mult *= provider(0);
                    }
                } else {
                    mult = 1;
                }
                consumeProbe<Ind + 1>(localState, target, mult, providers..., provider);
            });
        } else {
            consumeTarget(localState, target, multiplicity, std::make_index_sequence<sizeof...(Attrs)>{}, providers...);
        }
    }

    [[gnu::always_inline]] void prepareProbe(LocalState& localState, uint64_t key) {
        if constexpr (sizeof...(Probes) > 0)
            // only prepare for the first probe in the pipeline
            std::get<0>(probes).prepare(std::get<0>(localState.probes), key);
    }

    template <typename T>
    [[gnu::noinline]] static void callFinishConsume(T&& obj) {
        callFinishConsumeImpl(std::forward<T>(obj), 0);
    }

    template <typename T>
    static auto callFinishConsumeImpl(T&& obj, int) -> decltype(obj.finishConsume(), void()) {
        obj.finishConsume();
    }

    template <typename T>
    static void callFinishConsumeImpl(T&&, ...) {
        /* No-op if finishConsume() is not available */
    }

    template <typename T>
    [[gnu::noinline]] static void callFinalize(T&& obj, typename Target::LocalState& ls) {
        callFinalizeImpl(std::forward<T>(obj), ls, 0);
    }

    template <typename T>
    static auto callFinalizeImpl(T&& obj, typename Target::LocalState& ls, int) -> decltype(obj.finalize(ls), void()) {
        obj.finalize(ls);
    }

    template <typename T>
    static void callFinalizeImpl(T&&, typename Target::LocalState&, ...) {
        /* No-op if finishConsume() is not available */
    }

    /// Run the scan
    void operator()() {
        scan(
            [this](size_t workerId) {
                auto state = localStates.get(workerId);
                return state;
            },
            [this](void* localStateRaw, auto provider) __attribute__((always_inline)) {
                auto* localState = static_cast<LocalState*>(localStateRaw);
                assert(localState->initialized);
                uint64_t multiplicity = 1;
                consumeProbe(*localState, target, multiplicity, provider);
            },
            [this](void* localStateRaw, uint64_t key) __attribute__((always_inline)) {
                auto* localState = static_cast<LocalState*>(localStateRaw);
                assert(localState->initialized);
                prepareProbe(*localState, key);
            },
            [this](size_t workerId, void* localStateRaw, bool init) {
                auto* localState = static_cast<LocalState*>(localStateRaw);
                if (init) {
                    assert(!localState->initialized);
                    std::apply([&](auto&&... probes) {
                        new (reinterpret_cast<LocalState*>(localState)) LocalState(workerId, this, target, scan, probes...);
                    },
                               probes);
                    localState->initialized = true;
                } else {
                    assert(localState->initialized);
                    callFinalize(target, localState->target);
                }
            });
        callFinishConsume(target);
    }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
