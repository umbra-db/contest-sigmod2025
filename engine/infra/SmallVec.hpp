#pragma once
//---------------------------------------------------------------------------
#include "infra/helper/Span.hpp"
#include "infra/QueryMemory.hpp"
#include <algorithm>
#include <cassert>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename T, typename Allocator = querymemory::Allocator<T>>
class SmallVecBase : Allocator {
    protected:
    /// The pointer to data
    T* ptr;
    /// The capacity
    size_t cap;
    /// The size
    size_t len = 0;

    /// Is inline?
    constexpr bool isInline() const noexcept { return ptr == reinterpret_cast<const T*>(this + 1); }

    /// Constructor
    constexpr SmallVecBase(T* ptr, size_t cap) noexcept : ptr(ptr), cap(cap) {}

    public:
    /// Destructor
    ~SmallVecBase() {
        if (!isInline())
            doDelete(ptr, cap);
    }
    /// Get the size
    constexpr size_t size() const noexcept { return len; }
    /// Is empty?
    constexpr bool empty() const noexcept { return len == 0; }
    /// Get the capacity
    constexpr size_t capacity() const noexcept { return cap; }
    /// Get the data
    constexpr T* data() noexcept { return ptr; }
    /// Get the data
    constexpr const T* data() const noexcept { return ptr; }

    static void resetValues(T* st, T* en) {
        for (T* p = st; p != en; ++p)
            *p = {};
    }

    /// Grow
    void grow(size_t minCap = 0) {
        auto newCap = std::max(minCap, (cap + 1) * 2);
        T* newPtr = doNew(newCap);
        std::move(ptr, ptr + len, newPtr);
        if (!isInline())
            doDelete(ptr, cap);
        cap = newCap;
        ptr = newPtr;
    }
    /// Reserve capacity
    void reserve(size_t newCap) {
        if (newCap <= cap)
            return;
        grow(newCap);
    }

    /// Pop back
    void pop_back() {
        assert(!empty());
        ptr[--len] = {};
    }
    /// Push back
    void push_back(const T& value) {
        if (len == cap) [[unlikely]]
            grow();
        ptr[len++] = value;
    }
    /// Push back
    void push_back(T&& value) {
        if (len == cap) [[unlikely]]
            grow();
        ptr[len++] = std::move(value);
    }
    /// Resize
    void resize(size_t newSize, const T& value = T()) {
        if (newSize > len) {
            if (newSize > cap)
                grow(newSize);
            std::fill(ptr + len, ptr + newSize, value);
        } else if (newSize < len) {
            resetValues(ptr + newSize, ptr + len);
        }
        len = newSize;
    }
    /// Emplace back
    template <typename... Args>
    T& emplace_back(Args&&... args) {
        if (len == cap) [[unlikely]]
            grow();
        return ptr[len++] = T(std::forward<Args>(args)...);
    }

    /// Begin
    constexpr const T* begin() const noexcept { return ptr; }
    /// End
    constexpr const T* end() const noexcept { return ptr + len; }
    /// Front
    constexpr const T& front() const noexcept {
        assert(!empty());
        return *ptr;
    }
    /// Back
    constexpr const T& back() const noexcept {
        assert(!empty());
        return ptr[len - 1];
    }
    /// Begin
    constexpr T* begin() noexcept { return ptr; }
    /// End
    constexpr T* end() noexcept { return ptr + len; }
    /// Front
    constexpr T& front() noexcept {
        assert(!empty());
        return *ptr;
    }
    /// Back
    constexpr T& back() noexcept {
        assert(!empty());
        return ptr[len - 1];
    }
    /// Erase
    T* erase(T* first, T* last) {
        assert(first <= last);
        assert(first >= ptr && last <= ptr + len);
        std::move(last, ptr + len, first);
        resetValues(first + (ptr + len - last), ptr + len);
        len -= last - first;
        return first;
    }
    /// Subscript
    T& operator[](size_t index) {
        assert(index < len);
        return ptr[index];
    }
    /// Subscript
    const T& operator[](size_t index) const {
        assert(index < len);
        return ptr[index];
    }

    T* doNew(std::size_t sz) {
        T* ptr = new (Allocator::allocate(sz)) T[sz];
        return ptr;
    }
    void doDelete(T* ptr, std::size_t sz) {
        for (std::size_t i = 0; i < sz; ++i)
            ptr[i].~T();
        Allocator::deallocate(ptr, sz);
    }

    /// Comparison
    bool operator==(const SmallVecBase& other) const {
        if (len != other.len)
            return false;
        for (size_t i = 0; i < len; i++)
            if (ptr[i] != other.ptr[i])
                return false;
        return true;
    }
    /// Comparison
    bool operator!=(const SmallVecBase& other) const {
        return !operator==(other);
    }
    /// Cast to span
    operator engine::span<T>() const {
        return engine::span<T>(ptr, len);
    }
    /// Clear
    void clear() noexcept {
        resetValues(this->ptr, this->ptr + this->len);
        this->len = 0;
    }
    /// Insert range
    void insert(T* pos, T* first, T* last) {
        assert(pos >= ptr && pos <= ptr + len);
        assert(first <= last);
        size_t count = last - first;
        if (len + count > cap)
            grow(len + count);
        std::move_backward(pos, ptr + len, ptr + len + count);
        std::copy(first, last, pos);
        len += count;
    }
};
//---------------------------------------------------------------------------
template <typename T, std::size_t N = 8, typename Allocator = querymemory::Allocator<T>>
/// A small stack-based vector keeping the first N elements always on the stack
class SmallVec : public SmallVecBase<T, Allocator> {
    /// The inline data
    std::array<T, N> values{};

    using Base = SmallVecBase<T, Allocator>;
    using Base::resetValues;

    public:
    using value_type = T;
    /// Constructor
    constexpr SmallVec() noexcept : Base(reinterpret_cast<T*>(&values), N) {
        //static_assert(offsetof(SmallVec, values) == sizeof(Base), "isInline() should work");
    }
    /// Constructor
    constexpr SmallVec(size_t size, const T& value = {}) noexcept : Base(reinterpret_cast<T*>(&values), N) {
        this->reserve(size);
        for (size_t i = 0; i < size; i++)
            this->push_back(value);
    }
    /// Constructor
    constexpr SmallVec(std::initializer_list<T> list) noexcept : Base(reinterpret_cast<T*>(&values), N) {
        this->reserve(list.size());
        for (const T& value : list)
            this->push_back(value);
    }
    /// Move constructor
    SmallVec(SmallVec&& other) noexcept : Base(reinterpret_cast<T*>(&values), N) { *this = std::move(other); }
    /// Copy constructor
    SmallVec(const SmallVec& other) noexcept : Base(reinterpret_cast<T*>(&values), N) { *this = other; }
    /// Move assignment
    SmallVec& operator=(SmallVec&& other) noexcept {
        if (this == &other)
            return *this;
        this->clear();
        if (!this->isInline()) {
            this->doDelete(this->ptr, this->cap);
            this->ptr = values.data();
            this->cap = N;
        }
        if (other.isInline()) {
            std::move(other.ptr, other.ptr + other.len, values.data());
            this->ptr = values.data();
        } else {
            this->ptr = other.ptr;
        }
        this->cap = other.cap;
        this->len = other.len;
        other.ptr = other.values.data();
        other.cap = N;
        other.len = 0;
        return *this;
    }
    /// Copy assignment
    SmallVec& operator=(const SmallVec& other) noexcept {
        if (this == &other)
            return *this;
        this->clear();
        if (!this->isInline()) {
            this->doDelete(this->ptr, this->cap);
            this->ptr = values.data();
            this->cap = N;
        }
        if (this->cap < other.len) {
            T* newPtr = this->doNew(other.cap);
            std::copy(other.ptr, other.ptr + other.len, newPtr);
            this->ptr = newPtr;
            this->cap = other.cap;
        } else {
            std::copy(other.ptr, other.ptr + other.len, this->ptr);
        }
        this->len = other.len;
        return *this;
    }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
