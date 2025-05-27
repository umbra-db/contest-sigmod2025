#pragma once
//---------------------------------------------------------------------------
#include "infra/Util.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <utility>
//---------------------------------------------------------------------------
namespace engine {
class Scheduler {
    static __thread size_t currentWorker;

    public:
    struct Worker;
    struct Impl;

    class Task;
    template <typename Fun>
    class TaskImpl;

    /// Setup the scheduler
    static void setup();
    /// Teardown the scheduler
    static void teardown();
    /// Start a query
    static void start_query();
    /// End a query
    static void end_query();

    /// Run a parallel morsel task
    static void parallelImpl(size_t size, FunctionRef<void(size_t, size_t)> task, bool finalizeTask = false);
    /// Run a parallel morsel task
    template <typename Fun>
    static void parallelMorsel(size_t begin, size_t end, size_t morselSize, Fun&& task, bool finalizeTask = false) {
        return parallelImpl((end - begin + morselSize - 1) / morselSize, [&task, morselSize, begin](size_t workerId, size_t i) { return task(workerId, i >= ~0ull - 1 ? i : begin + i * morselSize); }, finalizeTask);
    }
    /// Run a parallel for task
    template <typename Fun>
    static void parallelFor(size_t begin, size_t end, size_t stepSize, size_t morselSize, Fun&& task) {
        assert(stepSize > 0);
        assert(morselSize > 0);
        assert(stepSize % morselSize == 0);
        return parallelMorsel(begin, end, stepSize, [stepSize, morselSize, &task](size_t workerId, size_t i) {
            for (size_t j = 0; j < stepSize; j += morselSize)
                task(workerId, i + j);
        });
    }
    /// Run a parallel for task
    template <typename Fun>
    static void parallelFor(size_t begin, size_t end, size_t stepSize, Fun&& task) {
        return parallelFor(begin, end, stepSize, 1, std::forward<Fun>(task));
    }
    /// Run a parallel for task
    template <typename Fun>
    static void parallelFor(size_t begin, size_t end, Fun&& task) {
        return parallelFor(begin, end, 1, std::forward<Fun>(task));
    }

    /// Get thread id
    static size_t threadId() noexcept { return currentWorker; }

    /// Get the hardware concurrency
    static size_t concurrency() noexcept;
    /// Get the ratio of cores we are using
    static size_t unusedRatio() noexcept;
};
//---------------------------------------------------------------------------
}
