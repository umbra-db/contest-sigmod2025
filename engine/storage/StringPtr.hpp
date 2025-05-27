#pragma once
//---------------------------------------------------------------------------
// Sigmod programming contest
// (c) 2025 Tobias Schmidt
//---------------------------------------------------------------------------
#include "infra/helper/Span.hpp"
#include "query/DataSource.hpp"
#include <cassert>
#include <cstdint>
#include <string_view>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
/// Representing three different kinds of strings
/// "short" strings:
///     - stored in their original location (on the original page)
///     - have a continous representation -> string_view
///  => store the length of the string in the least 15 bits, 16th bit MUST be zero
///  "very short" string:
///     - short strings with length <= 6
///     - stored within the remaining bits of the StringPtr
///     - have a continous representation
///  => store the length of the string in the last 15 bits, 16th bit MUST be zero
/// "long" strings:
///     - stored as array of original pages
///     - NO continous representation -> split across multiple pages
///  => store the number of pages in the last 15 bits, 16th bit MUST be one
struct StringPtr {
    /// The data
    std::array<std::byte, 8> data;

    uint64_t val() const {
        return __builtin_bit_cast(uint64_t, data);
    }
    StringPtr() = default;
    constexpr StringPtr(uint64_t val) : data(__builtin_bit_cast(decltype(data), (val))) {
    }
    /// Access the length
    uint16_t length() const {
        assert(!is_long());
        uint16_t len;
        memcpy(&len, data.data(), 2);
        return len;
    }
    /// Access the string
    const char* str() const {
        assert(!is_long());
        if (length() <= 6)
            return (char*) data.data() + 2;
        char* ptr = nullptr;
        memcpy(&ptr, data.data() + 2, 6);
        return ptr;
    }
    /// Access the string
    const char* strLong() const {
        assert(!is_long());
        assert(length() > 6);

        char* ptr = nullptr;
        memcpy(&ptr, data.data() + 2, 6);
        return ptr;
    }
    /// Access the string
    std::string_view strView() const { return {str(), length()}; }
    /// Is this string null?
    bool isNull() const { return memcmp(data.data(), null.data.data(), 8) == 0; }

    size_t num_pages() const {
        assert(is_long());
        uint16_t num;
        memcpy(&num, data.data(), 2);
        return num & ((1ul << 15) - 1);
    }
    /// Get the used pages of a long string
    engine::span<engine::DataSource::Page*> pages() const {
        assert(is_long());
        engine::DataSource::Page** ptr = nullptr;
        memcpy(&ptr, data.data() + 2, 6);
        return {ptr, num_pages()};
    }
    /// Access the beginning of the string
    std::string_view prefix() const {
        if (is_long()) {
            engine::DataSource::Page** page = nullptr;
            memcpy(&page, data.data() + 2, 6);
            return {(*page)->getLongString(), (*page)->numNotNull};
        } else
            return {str(), length()};
    }
    /// Materialize the whole long string into a continous in-memory string
    /// EXPENSIVE!!!! AVOID AT ALL COSTS!!!!
    std::string materialize_string() const {
        if (is_long()) {
            std::string str;
            str.reserve(1024);
            for (auto* page : pages())
                str += std::string_view(page->getLongString(), page->numNotNull);
            return str;
        } else
            return std::string(str(), length());
    }
    /// Try to compare to strings as efficiently as possible
    /// NOTE: this assumes that long-strings are represented in a canonicalized form
    /// - meaning all pages are filled up to the maximum
    bool operator==(StringPtr other) const {
        if (is_long() != other.is_long()) return false;
        if (!is_long())
            return std::string_view{str(), length()} == std::string_view{other.str(), length()};
        // two long strings
        if (num_pages() != other.num_pages()) return false;
        const auto pages_lhs = pages();
        const auto pages_rhs = other.pages();
        if (pages_lhs.back()->numNotNull != pages_rhs.back()->numNotNull) return false;
        for (size_t i = 0; i < num_pages(); i++) {
            if (std::string_view{pages_lhs[i]->getLongString(), pages_lhs[i]->numNotNull} !=
                std::string_view{pages_rhs[i]->getLongString(), pages_rhs[i]->numNotNull})
                return false;
        }
        return true;
    }

    bool is_long() const {
        uint16_t len;
        memcpy(&len, data.data(), 2);
        return len & 1ul << 15;
    }
    /// create from a raw string
    static StringPtr fromString(const char* str, size_t len) {
        assert(len <= (1ull << 15) - 1);
        assert((reinterpret_cast<uint64_t>(str) >> 48) == 0ull);
        uint64_t val = (reinterpret_cast<uint64_t>(str) << 16) | len;
        StringPtr ptr;
        memcpy(ptr.data.data(), &len, 2);
        if (len <= 6)
            memcpy(ptr.data.data() + 2, str, len);
        else
            memcpy(ptr.data.data() + 2, &str, 6);
        return ptr;
    }
    /// create from a string_view
    static StringPtr fromString(std::string_view sv) {
        return fromString(sv.data(), sv.length());
    }
    // create from an array of Pages
    static StringPtr fromLongString(engine::DataSource::Page** pages, size_t num_pages) {
        assert(num_pages <= (1ull << 15) - 1);
        assert((reinterpret_cast<uint64_t>(pages) >> 48) == 0ull);
        StringPtr str;
        uint16_t len = 1u << 15 | num_pages;
        memcpy(str.data.data(), &len, 2);
        memcpy(str.data.data() + 2, &pages, 6);
        return str;
    }

    static const StringPtr null;
};
//---------------------------------------------------------------------------
constexpr const StringPtr StringPtr::null = {0xffffffffffffffff};
}
