#include "infra/Scheduler.hpp"
#include "infra/PageMemory.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/Random.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#if __has_include(<hardware.h>)
#include <hardware.h>
#endif
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
[[gnu::always_inline]] static inline void yield() {
#if defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#elif defined(__PPC64__)
    __asm__ volatile("or 27,27,27");
#else
    __sync_synchronize();
#endif
}
//---------------------------------------------------------------------------
class Scheduler::Task {
    protected:
    virtual void executeImpl(size_t workerId) const noexcept = 0;

    public:
    constexpr Task() noexcept = default;
    virtual ~Task() noexcept = default;
    void execute(size_t workerId) noexcept {
        executeImpl(workerId);
    }
};
//---------------------------------------------------------------------------
template <typename Fun>
class Scheduler::TaskImpl : public Task {
    Fun fun;

    public:
    TaskImpl(Fun&& fun) noexcept : fun(std::move(fun)) {}
    TaskImpl(const Fun& fun) noexcept : fun(fun) {}
    ~TaskImpl() noexcept override = default;
    void executeImpl(size_t workerId) const noexcept override {
        fun(workerId);
    }
};
//---------------------------------------------------------------------------
namespace {
// The maximum partition shift in a hashtable
constexpr size_t parallelSet = 0;
//---------------------------------------------------------------------------
struct DeadTask : Scheduler::Task {
    void executeImpl(size_t) const noexcept override {
        // do nothing
    }
};
//---------------------------------------------------------------------------
DeadTask deadTask;
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
__thread size_t Scheduler::currentWorker = 0;
//---------------------------------------------------------------------------
struct Scheduler::Worker {
    std::atomic<bool> sleeping{true};
    size_t id;
    Impl* schedulerImpl = nullptr;
    std::thread myThread;

    Worker(Impl& schedulerImpl, size_t id) noexcept;

    void operator()();
};
//---------------------------------------------------------------------------
struct Scheduler::Impl {
    std::vector<std::unique_ptr<Worker>> workers;
    std::atomic<Task*> availableTask{nullptr};

    struct alignas(hardwareCachelineSize) {
        std::atomic<bool> setupDone{false};
        std::atomic<bool> maintenanceDone{true};
        std::atomic<bool> doMaintenance{true};
        bool pmDone = false, qmDone = false;
    };

    Impl() = default;
    ~Impl() noexcept;

    void start();
    void stop();

    void run(Task& task);

    void performMaintenance() {
        if (!setupDone.load()) {
            auto numThreads = concurrency() - 1;
            for (size_t i = 2; i < numThreads + 1; ++i) {
                workers.push_back(std::make_unique<Worker>(*this, i));
            }
            pagememory::setup();
            querymemory::setup();

            setupDone = true;

            if (numThreads == 0) {
                maintenanceDone = true;
                return;
            }
        }

        if (doMaintenance.load() && !(qmDone && pmDone)) {
            maintenanceDone = false;
            while (doMaintenance.load() && !(qmDone && pmDone)) {
                if (!qmDone) qmDone = querymemory::prefault();
                if (!pmDone) pmDone = pagememory::prefault();
            }
            maintenanceDone = true;
        }
    }
};
//---------------------------------------------------------------------------
Scheduler::Worker::Worker(Impl& schedulerImpl, size_t id) noexcept
    : id(id), schedulerImpl(&schedulerImpl), myThread([this] {
          currentWorker = this->id;
          (*this)();
      }) {
}
//---------------------------------------------------------------------------
Scheduler::Impl::~Impl() noexcept {
    stop();
}
//---------------------------------------------------------------------------
void Scheduler::Impl::start() {
    assert(!setupDone.load());

    auto numThreads = concurrency() - 1;
    workers.reserve(numThreads);
    if (numThreads == 0) {
        performMaintenance();
    } else {
        workers.push_back(std::make_unique<Worker>(*this, 1));
    }
}
//---------------------------------------------------------------------------
void Scheduler::Impl::stop() {
    availableTask.store(&deadTask);
    for (auto& worker : workers)
        worker->myThread.join();

    workers.clear();
    availableTask.store(nullptr);
}
//---------------------------------------------------------------------------
static Scheduler::Impl* schedulerImpl;
//---------------------------------------------------------------------------
template <typename Fun>
static void run(Fun&& task) {
    Scheduler::TaskImpl<std::decay_t<Fun>> taskImpl(std::forward<Fun>(task));
    schedulerImpl->run(taskImpl);
}
//---------------------------------------------------------------------------
void Scheduler::Impl::run(Task& task) {
    if (currentWorker != 0) {
        throw std::runtime_error("only the main thread can start tasks");
    }
    assert(!availableTask.load());
    availableTask.store(&task);

    task.execute(currentWorker);

    availableTask.store(nullptr);

    for (auto& w : workers)
        while (!w->sleeping.load());
    availableTask.store(nullptr);
}
//---------------------------------------------------------------------------
namespace {
struct alignas(hardwareCachelineSize) JobState {
    std::atomic<size_t> current;
    size_t end;
};
}
//---------------------------------------------------------------------------
/// Run a parallel morsel task
void Scheduler::parallelImpl(size_t size, FunctionRef<void(size_t, size_t)> task, bool finalizeTask) {
    size_t jobs = std::clamp<size_t>(size, 1, concurrency());

    JobState states[jobs];
    size_t step = (size + jobs - 1) / jobs;
    size_t begin = 0;
    for (size_t i = 0; i < jobs; i++) {
        states[i].current.store(begin, std::memory_order_release);
        states[i].end = std::min(begin + step, size);
        begin += step;
    }
    assert(states[jobs - 1].end == size);

    // TODO: right now we assign the threads to job round-robin, however we
    run([&](size_t workerId) {
        auto conc = concurrency();
        auto initialJobId = workerId / ((conc + jobs - 1) / jobs);
        auto currentJobId = initialJobId;
        bool first = finalizeTask;
        while (schedulerImpl->availableTask.load()) {
            auto* job = &states[currentJobId];

            if (job->current.load() < job->end) {
                // Work on the own job
                size_t i;
                while ((i = job->current.fetch_add(1, std::memory_order_relaxed)) < job->end) {
                    // Call initialize
                    if (first) {
                        first = false;
                        task(workerId, ~0ull - 1);
                    }
                    task(workerId, i);
                }
            }

            auto nextJobId = (currentJobId + 1) >= jobs ? 0 : currentJobId + 1;
            if (nextJobId == initialJobId) break;
            currentJobId = nextJobId;
        }
        // Call finalize
        if (first == false && finalizeTask)
            task(workerId, ~0ull);
    });
}
//---------------------------------------------------------------------------
void Scheduler::Worker::operator()() {
    auto conc = concurrency();
    Random rng(id);
    while (schedulerImpl->availableTask.load() != &deadTask) {
        Scheduler::Task* target = nullptr;
        {
            {
                if (threadId() % 4 == 0) {
                    for (size_t i = 0; i < 16 * 1024 && !schedulerImpl->availableTask.load(); i++)
                        yield();
                }
            }
            while (!schedulerImpl->availableTask.load()) {
                for (size_t i = 0; i < 32 && !schedulerImpl->availableTask.load(); i++)
                    yield();

                if (currentWorker == 1) {
                    schedulerImpl->performMaintenance();
                }
                std::this_thread::sleep_for(std::chrono::microseconds(rng.nextRange(12) + 1));
            }
        }

        sleeping.store(false);
        target = schedulerImpl->availableTask.load();
        if (target) {
            if (target == &deadTask)
                break;
            try {
                target->execute(id);
            } catch (...) {
                schedulerImpl->availableTask.store(nullptr);
                sleeping.store(true);
                continue;
            }
            if (schedulerImpl->availableTask.load())
                schedulerImpl->availableTask.store(nullptr);
        }
        sleeping.store(true);
    }
}
//---------------------------------------------------------------------------
int computeAffinityThreads() {
#if defined(SPC__CORE_COUNT) && defined(SPC__THREAD_COUNT)
    return std::min<int>(SPC__CORE_COUNT * 2, SPC__THREAD_COUNT);
#else
    /// This should only return the hardware threads within the numa node allocated to us
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    sched_getaffinity(0, sizeof(cpu_set), &cpu_set);

    int count = 0;
    for (int i = 0; i < CPU_SETSIZE; ++i) {
        if (CPU_ISSET(i, &cpu_set)) {
            ++count;
        }
    }

    return count;
#endif
}
//---------------------------------------------------------------------------
int getAffinityThreads() {
    static int affinityThreads = computeAffinityThreads();
    return affinityThreads;
}
//---------------------------------------------------------------------------
size_t Scheduler::concurrency() noexcept {
    // TODO: we might need to adjust this for powerpc
    auto ps = parallelSet;
    if (!ps)
        return getAffinityThreads();
    return ps;
}
//---------------------------------------------------------------------------
size_t Scheduler::unusedRatio() noexcept {
    return std::thread::hardware_concurrency() / getAffinityThreads();
}
//---------------------------------------------------------------------------
void Scheduler::setup() {
    schedulerImpl = new Impl;
    schedulerImpl->start();
}
//---------------------------------------------------------------------------
void Scheduler::teardown() {
    // destruct and construct schedulerImpl
    schedulerImpl->stop();
    delete schedulerImpl;
}
//---------------------------------------------------------------------------
void Scheduler::start_query() {
    schedulerImpl->doMaintenance.store(false);
    while (!schedulerImpl->setupDone.load()) [[unlikely]] {
        yield();
    }
    while (!schedulerImpl->maintenanceDone.load()) [[unlikely]] {
        yield();
    }
    // We observed doMaintenance == false and maintenanceDone == true at the same time
    // as only we can modify doMaintenance.
    // Even if the worker was right before maintenanceDone.store(false), it will notice that doMaintenance is false and abort
}
//---------------------------------------------------------------------------
void Scheduler::end_query() {
    assert(schedulerImpl->maintenanceDone.load());
    assert(!schedulerImpl->doMaintenance.load());

    if (concurrency() > 1) {
        schedulerImpl->maintenanceDone.store(true);
        schedulerImpl->doMaintenance.store(true);
    }
}
//---------------------------------------------------------------------------
}
