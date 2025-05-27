#pragma once
//---------------------------------------------------------------------------
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <string>
#include <utility>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class Mmap {
    char* data_ = nullptr;
    size_t size_ = 0;
    int file = -1;

    public:
    Mmap(const Mmap&) = delete;
    Mmap& operator=(const Mmap&) = delete;

    constexpr Mmap() noexcept = default;
    ~Mmap() noexcept {
        reset();
    }
    Mmap(Mmap&& other) noexcept {
        *this = std::move(other);
    }
    Mmap& operator=(Mmap&& other) noexcept {
        reset();
        std::swap(data_, other.data_);
        std::swap(size_, other.size_);
        std::swap(file, other.file);
        return *this;
    }

    static void prefault(void* data, size_t size);


    static Mmap mapFile(const std::string& fileName);
    static Mmap mapMemory(size_t size);

    void reset() noexcept;

    constexpr operator bool() const noexcept { return data_ != nullptr; }

    char* data() const { return data_; }
    size_t size() const { return size_; }
};
//---------------------------------------------------------------------------
}
