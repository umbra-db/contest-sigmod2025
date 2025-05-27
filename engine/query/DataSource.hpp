#pragma once
//---------------------------------------------------------------------------
#include "infra/Mmap.hpp"
#include "infra/helper/Span.hpp"
#include <span>
#include <vector>
#include <attribute.h>
#include <unordered_map>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct DataSource {
    static constexpr size_t PAGE_SIZE = 8192;
    struct alignas(8) Page {
        uint16_t numRows = 0;
        uint16_t numNotNull = 0;
        std::byte data[PAGE_SIZE - 4];

        /// Does the page have no nulls?
        bool hasNoNulls() const {
            return (numNotNull == numRows) || isAnyLongString();
        }
        /// Get the actual number of rows
        constexpr size_t getContainedRows() const {
            return numRows >= 0xfffe ? numRows - 0xfffe : numRows;
        }
        /// Get whether the tuple at index is null
        bool isNull(size_t ind) const {
            /// The last (numRows + 7)/8 bytes are used to store null flags
            auto* bitset = getNulls();
            return !(bitset[ind / 8] & (1 << (ind % 8)));
        }
        /// Get the nulls
        const uint8_t* getNulls() const { return reinterpret_cast<const uint8_t*>(data + PAGE_SIZE - 4 - (numRows + 7) / 8); }
        /// Get the nulls
        uint8_t* getNulls() { return reinterpret_cast<uint8_t*>(data + PAGE_SIZE - 4 - (numRows + 7) / 8); }
        /// Get the start of the data
        template <typename T>
        const T* getData() const { return reinterpret_cast<const T*>(data + (sizeof(T) == 8 ? 4 : 0)); }
        /// Get the start of the data
        template <typename T>
        T* getData() { return reinterpret_cast<T*>(data + (sizeof(T) == 8 ? 4 : 0)); }
        /// Get the value at index
        template <typename T>
        T get(size_t ind) const {
            return getData<T>()[ind];
        }
        /// Is this a long string page
        bool isLongStringStart() const {
            return numRows == 0xffff;
        }
        /// Is this a long string page
        bool isLongStringContinuation() const {
            return numRows == 0xfffe;
        }
        /// Is this a long string page
        bool isAnyLongString() const {
            return numRows >= 0xfffe;
        }
        /// Get the string data for short strings
        const char* getStrings() const {
            assert(!isAnyLongString());
            return reinterpret_cast<const char*>(data + numNotNull * sizeof(uint16_t));
        }
        /// Get the string data for short strings
        char* getStrings() {
            assert(!isAnyLongString());
            return reinterpret_cast<char*>(data + numNotNull * sizeof(uint16_t));
        }
        /// Get the string data for long strings
        const char* getLongString() const {
            assert(isAnyLongString());
            return reinterpret_cast<const char*>(data);
        }
        /// Get the string data for long strings
        char* getLongString() {
            assert(isAnyLongString());
            return reinterpret_cast<char*>(data);
        }
    };
    static_assert(sizeof(Page) == PAGE_SIZE);

    struct Header;
    struct TableHeader;
    struct ColumnHeader;

    struct Column {
        DataType type;
        engine::span<Page* const> pages;
        std::vector<Page*> pagesStorage;
    };
    struct Table {
        uint64_t numRows;
        std::vector<Column> columns;
        std::string name;
        static constexpr size_t nameLenLimit = 1024 - 3 * sizeof(uint64_t);
        static std::string fixName(std::string name);
    };
    /// The vector of relations
    std::vector<Table> relations;
    /// File mapping
    Mmap fileMapping;

    void serialize(const std::string& filename) &&;
    static DataSource deserialize(const std::string& filename);
};
//---------------------------------------------------------------------------
}
