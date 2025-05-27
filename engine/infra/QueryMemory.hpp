#pragma once
#include <cassert>
#include <cstdlib>
#include <limits>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <memory>
//---------------------------------------------------------------------------
namespace engine::querymemory {
//---------------------------------------------------------------------------
/// Setup the query memory
void setup();
//---------------------------------------------------------------------------
/// Prefault the page memory
bool prefault();
//---------------------------------------------------------------------------
/// End a query
void end_query();
//---------------------------------------------------------------------------
/// Allocate a page
void* allocate(size_t bytes);
//---------------------------------------------------------------------------
/// std::allocator like wrapper for querymemory::allocator
template <typename T>
struct Allocator {
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    Allocator() noexcept = default;
    template <typename U>
    Allocator(const Allocator<U>&) noexcept {}

    T* allocate(std::size_t n) {
        return static_cast<T*>(::engine::querymemory::allocate(n * sizeof(T)));
    }

    void deallocate(T* p, std::size_t) noexcept { }

    template <typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* p) {
        p->~U();
    }

    template <typename U>
    struct rebind {
        using other = Allocator<U>;
    };
};
//---------------------------------------------------------------------------
struct Deleter {
    template <typename T>
    void operator()(T* p) const {
        if (p) {
            p->~T();
        }
    }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace engine {
template <typename T>
using Vector = std::vector<T, querymemory::Allocator<T>>;
template <typename T>
using UniquePtr = std::unique_ptr<T, querymemory::Deleter>;
template <typename T, typename... Args>
UniquePtr<T> makeUnique(Args&&... args) {
    return UniquePtr<T>(new (querymemory::allocate(sizeof(T))) T(std::forward<Args>(args)...));
}
template <typename T, typename Hash = std::hash<T>, typename Equal = std::equal_to<T>>
using UnorderedSet = std::unordered_set<T, Hash, Equal, querymemory::Allocator<T>>;
template <typename K, typename V, typename Hash = std::hash<K>, typename Equal = std::equal_to<K>>
using UnorderedMap = std::unordered_map<K, V, Hash, Equal, querymemory::Allocator<std::pair<const K, V>>>;
}
//---------------------------------------------------------------------------
