#include "infra/PageMemory.hpp"
#include "infra/Mmap.hpp"
#include "infra/QueryMemory.hpp"
#include "infra/Scheduler.hpp"
#include "infra/Util.hpp"
#include "query/DataSource.hpp"
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
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
struct PageMemory {
    /// MMapped memory region
    engine::Mmap memory;
    /// The data
    char* data = nullptr;
    /// The bitmap to track the used pages
    std::vector<uint64_t> bitmap;
    /// The initial number of pages
    size_t pageCount = 0;
    /// The number of pages to prefault
    size_t prefaultCount = 0;
    /// The used number of pages
    std::atomic<size_t> pages = 0;
    /// Have we prefaulted?
    std::atomic<size_t> prefaultedPages = 0;

    /// Perform an allocation
    void* allocate(size_t numPages = 1) {
        // memory order relaxed is sufficient as we do not care about ordering with
        // respect to other atomic operations at all...
        size_t idx = pages.fetch_add(numPages, std::memory_order_relaxed);

        // Fast path
        if (idx + numPages <= pageCount) [[likely]] {
            auto* res = data + (idx * engine::DataSource::PAGE_SIZE);
            return res;
        } else {
            if (idx < pageCount) [[unlikely]] {
                // Idx was less than pageCount, but we failed to allocate because the requested size was too large
                // Mark the last bits as deleted as we cannot use them
                for (size_t i = idx; i < pageCount; i++)
                    __atomic_fetch_or(&bitmap[i / 64], (1ul << (i % 64)), __ATOMIC_RELAXED);
            }
            return std::malloc(engine::DataSource::PAGE_SIZE * numPages);
        }
    }

    /// Check if a pointer is contained in the memory region
    bool contains(void* ptr) const {
        return memory && data <= ptr && ptr < data + pageCount * engine::DataSource::PAGE_SIZE;
    }
    /// Prefault the memory
    bool prefault() {
        constexpr size_t prefaultSize = 64 * 1024 / engine::DataSource::PAGE_SIZE;
        if (!memory) return true;

        prefaultedPages = std::max(pages.load(), prefaultedPages.load());
        if (prefaultedPages.load() < prefaultCount) {
            size_t p = prefaultedPages.fetch_add(prefaultSize);
            assert(p >= pages.load());
            engine::Mmap::prefault(data + p * engine::DataSource::PAGE_SIZE, prefaultSize * engine::DataSource::PAGE_SIZE);
            return false;
        }
        return true;
    }
    /// Perform a deallocation
    void deallocate(void* ptr, size_t size) {
        if (reinterpret_cast<uint64_t>(ptr) % 4096 != 0) [[unlikely]] {
            assert(reinterpret_cast<uint64_t>(ptr) % engine::DataSource::PAGE_SIZE == 16);
            char* p = static_cast<char*>(ptr) - 16;
            // This is a stolen pointer
            auto numPages = engine::unalignedLoad<uint64_t>(p);
            auto idx = (p - data) / engine::DataSource::PAGE_SIZE;
            // We can do this more efficiently but this is not really needed
            for (size_t i = idx; i < idx + numPages; i++) {
                assert(i < pages.load());
                assert(((__atomic_load_n(&bitmap[i / 64], __ATOMIC_SEQ_CST) >> (i % 64)) & 1) == 0);
                __atomic_fetch_or(&bitmap[i / 64], (1ul << (i % 64)), __ATOMIC_RELAXED);
            }
            return;
        }
        size_t idx = (static_cast<char*>(ptr) - data) / engine::DataSource::PAGE_SIZE;
        assert(idx < pages.load());
        assert(((__atomic_load_n(&bitmap[idx / 64], __ATOMIC_SEQ_CST) >> (idx % 64)) & 1) == 0);

        __atomic_fetch_or(&bitmap[idx / 64], (1ul << (idx % 64)), __ATOMIC_RELAXED);
    }
};
PageMemory* pageMemory = nullptr;
//---------------------------------------------------------------------------
/// Perform a deallocation
void free(void* ptr, size_t size) {
    // only skip pages that were allocated with our allocator
    if (pageMemory && pageMemory->contains(ptr)) {
        pageMemory->deallocate(ptr, size);
    } else {
        std::free(ptr);
    }
}
//---------------------------------------------------------------------------
struct alignas(engine::hardwareCachelineSize) LocalMemory {
    /// The number of pages to allocate for the local allocator
    static constexpr size_t numPages = 16;

    /// The begin
    char* begin = nullptr;
    /// The end
    char* end = nullptr;
};
std::vector<LocalMemory>* localMemory = nullptr;
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static __thread bool stealAllocations = false;
namespace engine::pagememory {
//---------------------------------------------------------------------------
AllocationStealer::AllocationStealer() noexcept {
    stealAllocations = true;
}
//---------------------------------------------------------------------------
AllocationStealer::~AllocationStealer() noexcept {
    stealAllocations = false;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void operator delete(void* ptr, std::size_t size) noexcept {
    free(ptr, size);
}
void operator delete(void* ptr) noexcept {
    free(ptr, engine::DataSource::PAGE_SIZE);
}
void operator delete(void* ptr, std::align_val_t) noexcept {
    free(ptr, engine::DataSource::PAGE_SIZE);
}
void operator delete(void* ptr, std::size_t size, std::align_val_t) noexcept {
    free(ptr, size);
}
void operator delete[](void* ptr) noexcept {
    free(ptr, engine::DataSource::PAGE_SIZE);
}
void operator delete[](void* ptr, std::size_t size) noexcept {
    free(ptr, size);
}
void operator delete[](void* ptr, std::align_val_t) noexcept {
    free(ptr, engine::DataSource::PAGE_SIZE);
}
void operator delete[](void* ptr, std::size_t size, std::align_val_t) noexcept {
    free(ptr, size);
}
//---------------------------------------------------------------------------
void* stolenAlloc(std::size_t size);
//---------------------------------------------------------------------------
void* operator new(std::size_t size) {
    if (stealAllocations) [[unlikely]]
        return stolenAlloc(size);
    auto res = std::malloc(size);
    if (res == nullptr)
        throw std::bad_alloc();
    return res;
}
void* operator new[](std::size_t size) {
    if (stealAllocations) [[unlikely]]
        return stolenAlloc(size);
    auto res = std::malloc(size);
    if (res == nullptr)
        throw std::bad_alloc();
    return res;
}
void* operator new(std::size_t size, std::align_val_t align) {
    auto res = std::aligned_alloc(static_cast<size_t>(align), size);
    if (res == nullptr)
        throw std::bad_alloc();
    return res;
}
void* operator new[](std::size_t size, std::align_val_t align) {
    auto res = std::aligned_alloc(static_cast<size_t>(align), size);
    if (res == nullptr)
        throw std::bad_alloc();
    return res;
}
//---------------------------------------------------------------------------
void* stolenAlloc(std::size_t size) {
    if (size <= engine::DataSource::PAGE_SIZE) [[likely]] {
        return pageMemory->allocate();
    }
    std::size_t requiredSize = size + 16;
    uint64_t numPages = (requiredSize + engine::DataSource::PAGE_SIZE - 1) / engine::DataSource::PAGE_SIZE;
    char* ptr = static_cast<char*>(pageMemory->allocate(numPages));
    engine::unalignedStore(ptr, numPages);
    return ptr + 16;
}
//---------------------------------------------------------------------------
namespace engine::pagememory {
//---------------------------------------------------------------------------
void setup() {
    if (!pageMemory) {
        pageMemory = new PageMemory;
        localMemory = new std::vector<LocalMemory>;
    }

    if (pageMemory->data != nullptr) {
        pageMemory->memory.reset();
        pageMemory->data = nullptr;
        pageMemory->bitmap.clear();
        pageMemory->pageCount = 0;
        pageMemory->prefaultCount = 0;
        pageMemory->pages = 0;
        pageMemory->prefaultedPages = 0;
        localMemory->clear();
    }

    size_t ram = sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE);

    // 20% of total available ram should be fine
    auto requestedMem = memorySize ? memorySize : ram / Scheduler::unusedRatio() / 2;
    pageMemory->pageCount = (requestedMem + DataSource::PAGE_SIZE - 1) / DataSource::PAGE_SIZE;
    pageMemory->prefaultCount = std::min<size_t>(requestedMem / 2, (1ul << 30)) / DataSource::PAGE_SIZE;

    pageMemory->memory = Mmap::mapMemory(pageMemory->pageCount * DataSource::PAGE_SIZE);
    pageMemory->data = reinterpret_cast<char*>(reinterpret_cast<uintptr_t>(pageMemory->memory.data() + DataSource::PAGE_SIZE - 1) & -DataSource::PAGE_SIZE);
    pageMemory->pageCount -= 1;
    pageMemory->bitmap.resize((pageMemory->pageCount + 63) / 64);
    memset(pageMemory->bitmap.data(), 0, pageMemory->bitmap.size() * sizeof(uint64_t));

    localMemory->resize(Scheduler::concurrency());
}
//---------------------------------------------------------------------------
bool prefault() {
    return pageMemory->prefault();
}
//---------------------------------------------------------------------------
void start_query() {
    for (auto& mem : *localMemory) {
        while (mem.begin != mem.end) {
            pageMemory->deallocate(mem.begin, DataSource::PAGE_SIZE);
            mem.begin += DataSource::PAGE_SIZE;
        }
        mem.begin = nullptr;
        mem.end = nullptr;
    }

    auto begin = std::min<size_t>(pageMemory->pages.load(), pageMemory->pageCount);
    // Pages upto begin have been allocated in the bump allocator
    // The deleted pages have been marked in the bitmap
    // We now try to move the iterator of the bump allocator back as long as pages are deleted
    if (!begin)
        return;
    if (begin % 64) {
        pageMemory->bitmap[begin / 64] |= ~((1ull << (begin % 64)) - 1);
        begin += 64 - (begin % 64);
    }
    for (uint64_t i = (begin - 1) / 64; i != ~0ull; i--) {
        if (pageMemory->bitmap[i] != ~0ull) {
            auto bitPos = 64 - __builtin_clzll(~pageMemory->bitmap[i]);
            pageMemory->bitmap[i] = pageMemory->bitmap[i] & ((1ull << bitPos) - 1);
            pageMemory->pages.store(i * 64 + bitPos, std::memory_order_relaxed);
            return;
        }
        pageMemory->bitmap[i] = 0;
    }
    pageMemory->pages.store(0, std::memory_order_relaxed);
}
//---------------------------------------------------------------------------
void* allocate()
// Allocate a page
{
    auto& local = (*localMemory)[Scheduler::threadId()];

    if (local.begin == local.end) {
        local.begin = static_cast<char*>(pageMemory->allocate(LocalMemory::numPages));
        local.end = local.begin + LocalMemory::numPages * DataSource::PAGE_SIZE;
    }

    auto p = local.begin;
    local.begin += DataSource::PAGE_SIZE;
    assert(local.begin <= local.end);

    return p;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
