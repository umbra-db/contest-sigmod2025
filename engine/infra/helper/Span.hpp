#pragma once
//---------------------------------------------------------------------------
#include <array>
#include <cstddef>
#include <cassert>
#include <type_traits>
#include <iterator>
#include <cstdint>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename T>
class span {
    public:
    using value_type = T;
    using iterator = T*;
    using const_iterator = const T*;

    private:
    T* data_;
    size_t size_;

    public:
    constexpr span(T* data, size_t size) noexcept : data_(data), size_(size) {}
    constexpr span(T* begin, T* end) noexcept : data_(begin), size_(end - begin) {}
    template <typename Container>
    constexpr span(Container& arr) noexcept : data_(arr.data()), size_(arr.size()) {}
    template <typename Container>
    constexpr span(const Container& arr) noexcept : data_(arr.data()), size_(arr.size()) {}
    constexpr span() noexcept : data_(nullptr), size_(0) {}

    constexpr size_t size() const noexcept { return size_; }
    constexpr bool empty() const noexcept { return size_ == 0; }
    constexpr T& operator[](size_t i) const noexcept {
        assert(i < size_);
        return data_[i];
    }
    constexpr T& front() const noexcept {
        assert(size_ > 0);
        return data_[0];
    }
    constexpr T& back() const noexcept {
        assert(size_ > 0);
        return data_[size_ - 1];
    }
    constexpr T* data() const noexcept { return data_; }

    constexpr auto begin() const noexcept { return data_; }
    constexpr auto end() const noexcept { return data_ + size_; }
};
//---------------------------------------------------------------------------
template <class Container>
span(Container&) -> span<typename Container::value_type>;
//---------------------------------------------------------------------------
template <class Container>
span(const Container&) -> span<const typename Container::value_type>;
//---------------------------------------------------------------------------
} // namespace engine
//---------------------------------------------------------------------------
