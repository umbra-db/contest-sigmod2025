#include "infra/Mmap.hpp"
#include "infra/Util.hpp"
#include "infra/Scheduler.hpp"
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
void Mmap::prefault(void* data, size_t size) {
    char* mem = static_cast<char*>(data);
    for (std::size_t i = 0; i < size; i += 4096)
        mem[i] = 0;
}
//---------------------------------------------------------------------------
Mmap Mmap::mapFile(const std::string& filename) {
    Mmap result;
    result.file = open(filename.c_str(), O_RDONLY);
    if (result.file == -1) {
        return result;
    }
    result.size_ = lseek(result.file, 0, SEEK_END);
    if (result.size_ == -1) {
        close(result.file);
        result.file = -1;
        return result;
    }
    result.data_ = static_cast<char*>(mmap(nullptr, result.size_, PROT_READ, MAP_SHARED, result.file, 0));
    if (result.data_ == MAP_FAILED) {
        close(result.file);
        result.file = -1;
        result.size_ = 0;
        result.data_ = nullptr;
    }
#ifdef SIGMOD_LOCAL
    size_t morselSize = 1ull << 21;
    Scheduler::parallelMorsel(0, result.size_, morselSize, [&](size_t workerId, size_t pos) {
        auto end = std::min(pos + morselSize, result.size_);
        madvise(result.data_ + pos, end - pos, MADV_POPULATE_READ);
    });
#endif
    return result;
}
//---------------------------------------------------------------------------
Mmap Mmap::mapMemory(size_t size) {
    Mmap result;
    result.size_ = size;
#ifdef NDEBUG
    result.data_ = static_cast<char*>(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (size >= 2'000'000)
        madvise(result.data_, size, MADV_HUGEPAGE);
#else
    result.data_ = static_cast<char*>(aligned_alloc(4096, size));
#endif
    return result;
}
//---------------------------------------------------------------------------
void Mmap::reset() noexcept {
    if (data_) {
#ifdef NDEBUG
        munmap(data_, size_);
#else
        if (file)
            munmap(data_, size_);
        else
            free(data_);
#endif
        data_ = nullptr;
        size_ = 0;
    }
    if (file != -1) {
        close(file);
        file = -1;
    }
}
//---------------------------------------------------------------------------
}
