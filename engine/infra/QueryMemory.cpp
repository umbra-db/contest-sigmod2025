#include "infra/QueryMemory.hpp"
#include "infra/Mmap.hpp"
#include "infra/Scheduler.hpp"
#include "query/DataSource.hpp"
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <unistd.h>
//---------------------------------------------------------------------------
// leak sanitizer crashes... therefore disable it for now!
// extern "C" const char* __asan_default_options() { return "detect_leaks=0"; }
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
constexpr size_t memorySize = 0;
//---------------------------------------------------------------------------
/// Bump allocator for allocating query memory
struct QueryMemory {
    /// MMapped memory region
    engine::Mmap memory;
    /// The initial number of pages
    size_t pageCount = 0;
    /// The number of pages to prefault
    size_t prefaultCount = 0;
    /// The used number of pages
    std::atomic<size_t> pages = 0;
    /// Have we prefaulted?
    std::atomic<size_t> prefaultedPages = 0;

    /// Prefault the memory
    bool prefault() {
        constexpr size_t prefaultSize = 64 * 1024 / engine::DataSource::PAGE_SIZE;
        if (!memory) return true;

        prefaultedPages = std::max(pages.load(), prefaultedPages.load());
        if (prefaultedPages.load() < prefaultCount) {
            size_t p = prefaultedPages.fetch_add(prefaultSize);
            assert(p >= pages.load());
            engine::Mmap::prefault(memory.data() + p * engine::DataSource::PAGE_SIZE, prefaultSize * engine::DataSource::PAGE_SIZE);
            return false;
        }
        return true;
    }
    /// Perform an allocation
    void* allocate(size_t count) {
        // memory order relaxed is sufficient as we do not care about ordering with
        // respect to other atomic operations at all...
        size_t idx = pages.fetch_add(count, std::memory_order_relaxed);

        assert(idx + count <= pageCount);
        return memory.data() + (idx * engine::DataSource::PAGE_SIZE);
    }
};

QueryMemory* queryMemory = nullptr;
//---------------------------------------------------------------------------
struct alignas(engine::hardwareCachelineSize) LocalMemory {
    /// The number of pages to allocate for the local allocator
    static constexpr size_t numPages = 128;

    void* memory = nullptr;
    /// The byte count
    size_t byteCount = 0;
    /// The used number of pages
    size_t bytes = 0;
};
std::vector<LocalMemory>* localMemory = nullptr;
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace engine::querymemory {
//---------------------------------------------------------------------------
bool prefault() {
    return queryMemory->prefault();
}
//---------------------------------------------------------------------------
void end_query()
// End a query
{
    queryMemory->pages = LocalMemory::numPages * localMemory->size();
    for (auto& mem : *localMemory) {
        mem.bytes = 0;
    }
}
//---------------------------------------------------------------------------
void* allocate(size_t bytes)
// Allocate a page
{
    auto& local = (*localMemory)[Scheduler::threadId()];

    constexpr size_t alignment = std::max<size_t>(hardwareCachelineSize, 16);
    bytes = (bytes + alignment - 1) & -alignment;
    if (bytes < DataSource::PAGE_SIZE && local.bytes + bytes <= local.byteCount) {
        auto offset = local.bytes;
        local.bytes += bytes;
        assert(local.memory);
        return static_cast<char*>(local.memory) + offset;
    }
    return queryMemory->allocate((bytes + DataSource::PAGE_SIZE - 1) / DataSource::PAGE_SIZE);
}
//---------------------------------------------------------------------------
void setup()
// Setup the query memory
{
    if (!queryMemory) {
        queryMemory = new QueryMemory;
        localMemory = new std::vector<LocalMemory>;
    }

    if (queryMemory->memory.data() != nullptr) {
        queryMemory->memory.reset();
        queryMemory->pageCount = 0;
        queryMemory->prefaultCount = 0;
        queryMemory->pages = 0;
        queryMemory->prefaultedPages = 0;
        localMemory->clear();
    }
    size_t ram = sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE);

    // 20% of total available ram should be fine
    auto requestedMem = memorySize ? memorySize : ram / Scheduler::unusedRatio() * 4 / 5;
    queryMemory->pageCount = (requestedMem + DataSource::PAGE_SIZE - 1) / DataSource::PAGE_SIZE;
    queryMemory->prefaultCount = std::min<size_t>(requestedMem / 2, (1ul << 30)) / DataSource::PAGE_SIZE;

    fprintf(stderr, "Total RAM: %luGB\n", ram / (1ul << 30));
    fprintf(stderr, "Allocator manages %lu pages (%luGB)\n", queryMemory->pageCount, queryMemory->pageCount * DataSource::PAGE_SIZE / (1ul << 30));

    queryMemory->memory = Mmap::mapMemory(queryMemory->pageCount * DataSource::PAGE_SIZE);
    assert((static_cast<size_t>(queryMemory->memory) & ~127ul) == 0); // max alignment

    localMemory->resize(Scheduler::concurrency());
    for (auto& mem : *localMemory) {
        mem.memory = queryMemory->allocate(LocalMemory::numPages);
        mem.byteCount = LocalMemory::numPages * DataSource::PAGE_SIZE;
    }
    assert(queryMemory->pages == LocalMemory::numPages * localMemory->size());
    Mmap::prefault(queryMemory->memory.data(), queryMemory->pages * DataSource::PAGE_SIZE);
    queryMemory->prefaultedPages = queryMemory->pages.load();
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
