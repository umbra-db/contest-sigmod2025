/*
 * Copyright 2025 Matthias Boehm, TU Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// API of the SIGMOD 2025 Programming Contest,
// See https://sigmod-contest-2025.github.io/index.html
#pragma once

#include <attribute.h>
#include <statement.h>
// #include <table.h>

// supported attribute data types

enum class NodeType {
    HashJoin,
    Scan,
};

struct ScanNode {
    size_t base_table_id;
};

struct JoinNode {
    bool   build_left;
    size_t left;
    size_t right;
    size_t left_attr;
    size_t right_attr;
};

struct PlanNode {
    std::variant<ScanNode, JoinNode>          data;
    std::vector<std::tuple<size_t, DataType>> output_attrs;

    PlanNode(std::variant<ScanNode, JoinNode>     data,
        std::vector<std::tuple<size_t, DataType>> output_attrs)
    : data(std::move(data))
    , output_attrs(std::move(output_attrs)) {}
};

constexpr size_t PAGE_SIZE = 8192;

struct alignas(8) Page {
    std::byte data[PAGE_SIZE];
};

struct Column {
    DataType           type;
    std::vector<Page*> pages;

    Page* new_page() {
        auto ret = new Page;
        pages.push_back(ret);
        return ret;
    }

    Column(DataType data_type)
    : type(data_type)
    , pages() {}

    Column(Column&& other) noexcept
    : type(other.type)
    , pages(std::move(other.pages)) {
        other.pages.clear();
    }

    Column& operator=(Column&& other) noexcept {
        if (this != &other) {
            for (auto* page: pages) {
                delete page;
            }
            type  = other.type;
            pages = std::move(other.pages);
            other.pages.clear();
        }
        return *this;
    }

    Column(const Column&)            = delete;
    Column& operator=(const Column&) = delete;

    ~Column() {
        for (auto* page: pages) {
            delete page;
        }
    }
};

struct ColumnarTable {
    size_t              num_rows{0};
    std::vector<Column> columns;
};

std::tuple<std::vector<std::vector<Data>>, std::vector<DataType>> from_columnar(
    const ColumnarTable& table);
ColumnarTable from_table(const std::vector<std::vector<Data>>& table,
    const std::vector<DataType>&                               data_types);

struct Plan {
    std::vector<PlanNode>      nodes;
    std::vector<ColumnarTable> inputs;
    // std::vector<Table>         tables;
    size_t root;

    size_t new_join_node(bool                     build_left,
        size_t                                    left,
        size_t                                    right,
        size_t                                    left_attr,
        size_t                                    right_attr,
        std::vector<std::tuple<size_t, DataType>> output_attrs) {
        JoinNode join{
            .build_left = build_left,
            .left       = left,
            .right      = right,
            .left_attr  = left_attr,
            .right_attr = right_attr,
        };
        auto ret = nodes.size();
        nodes.emplace_back(join, std::move(output_attrs));
        return ret;
    }

    size_t new_scan_node(size_t                   base_table_id,
        std::vector<std::tuple<size_t, DataType>> output_attrs) {
        ScanNode scan{.base_table_id = base_table_id};
        auto     ret = nodes.size();
        nodes.emplace_back(scan, std::move(output_attrs));
        return ret;
    }

    size_t new_input(ColumnarTable input) {
        auto ret = inputs.size();
        inputs.emplace_back(std::move(input));
        return ret;
    }
};

template <class T>
struct ColumnInserter {
    Column&              column;
    size_t               last_page_idx = 0;
    uint16_t             num_rows      = 0;
    size_t               data_end      = data_begin();
    std::vector<uint8_t> bitmap;

    constexpr static size_t data_begin() {
        if (sizeof(T) < 4) {
            return 4;
        } else {
            return sizeof(T);
        }
    }

    ColumnInserter(Column& column)
    : column(column) {
        bitmap.resize(PAGE_SIZE);
    }

    std::byte* get_page() {
        if (last_page_idx == column.pages.size()) [[unlikely]] {
            column.new_page();
        }
        auto* page = column.pages[last_page_idx];
        return page->data;
    }

    void save_page() {
        auto* page                         = get_page();
        *reinterpret_cast<uint16_t*>(page) = num_rows;
        *reinterpret_cast<uint16_t*>(page + 2) =
            static_cast<uint16_t>((data_end - data_begin()) / sizeof(T));
        size_t bitmap_size = (num_rows + 7) / 8;
        memcpy(page + PAGE_SIZE - bitmap_size, bitmap.data(), bitmap_size);
        ++last_page_idx;
        num_rows = 0;
        data_end = data_begin();
    }

    void set_bitmap(size_t idx) {
        size_t byte_idx   = idx / 8;
        size_t bit_idx    = idx % 8;
        bitmap[byte_idx] |= (0x1 << bit_idx);
    }

    void unset_bitmap(size_t idx) {
        size_t byte_idx   = idx / 8;
        size_t bit_idx    = idx % 8;
        bitmap[byte_idx] &= ~(0x1 << bit_idx);
    }

    void insert(T value) {
        if (data_end + 4 + num_rows / 8 + 1 > PAGE_SIZE) [[unlikely]] {
            save_page();
        }
        auto* page                              = get_page();
        *reinterpret_cast<T*>(page + data_end)  = value;
        data_end                               += sizeof(T);
        set_bitmap(num_rows);
        ++num_rows;
    }

    void insert_null() {
        if (data_end + num_rows / 8 + 1 > PAGE_SIZE) [[unlikely]] {
            save_page();
        }
        unset_bitmap(num_rows);
        ++num_rows;
    }

    void finalize() {
        if (num_rows != 0) {
            save_page();
        }
    }
};

template <>
struct ColumnInserter<std::string> {
    Column&              column;
    size_t               last_page_idx = 0;
    uint16_t             num_rows      = 0;
    uint16_t             data_size     = 0;
    size_t               offset_end    = 4;
    std::vector<char>    data;
    std::vector<uint8_t> bitmap;

    constexpr static size_t offset_begin() { return 4; }

    ColumnInserter(Column& column)
    : column(column) {
        data.resize(PAGE_SIZE);
        bitmap.resize(PAGE_SIZE);
    }

    std::byte* get_page() {
        if (last_page_idx == column.pages.size()) [[unlikely]] {
            column.new_page();
        }
        auto* page = column.pages[last_page_idx];
        return page->data;
    }

    void save_long_string(std::string_view value) {
        size_t offset     = 0;
        auto   first_page = true;
        while (offset < value.size()) {
            auto* page = get_page();
            if (first_page) {
                *reinterpret_cast<uint16_t*>(page) = 0xffff;
                first_page                         = false;
            } else {
                *reinterpret_cast<uint16_t*>(page) = 0xfffe;
            }
            auto page_data_len = std::min(value.size() - offset, PAGE_SIZE - 4);
            *reinterpret_cast<uint16_t*>(page + 2) = page_data_len;
            memcpy(page + 4, value.data() + offset, page_data_len);
            offset += page_data_len;
            ++last_page_idx;
        }
    }

    void save_page() {
        auto* page                         = get_page();
        *reinterpret_cast<uint16_t*>(page) = num_rows;
        *reinterpret_cast<uint16_t*>(page + 2) =
            static_cast<uint16_t>((offset_end - offset_begin()) / 2);
        size_t bitmap_size = (num_rows + 7) / 8;
        memcpy(page + offset_end, data.data(), data_size);
        memcpy(page + PAGE_SIZE - bitmap_size, bitmap.data(), bitmap_size);
        ++last_page_idx;
        num_rows   = 0;
        data_size  = 0;
        offset_end = offset_begin();
    }

    void set_bitmap(size_t idx) {
        size_t byte_idx   = idx / 8;
        size_t bit_idx    = idx % 8;
        bitmap[byte_idx] |= (0x1 << bit_idx);
    }

    void unset_bitmap(size_t idx) {
        size_t byte_idx   = idx / 8;
        size_t bit_idx    = idx % 8;
        bitmap[byte_idx] &= ~(0x1 << bit_idx);
    }

    void insert(std::string_view value) {
        if (value.size() > PAGE_SIZE - 7) {
            if (num_rows > 0) {
                save_page();
            }
            save_long_string(value);
        } else {
            if (offset_end + sizeof(uint16_t) + data_size + value.size() + num_rows / 8 + 1
                > PAGE_SIZE) {
                save_page();
            }
            memcpy(data.data() + data_size, value.data(), value.size());
            data_size  += static_cast<uint16_t>(value.size());
            auto* page  = get_page();
            *reinterpret_cast<uint16_t*>(page + offset_end)  = data_size;
            offset_end                                      += sizeof(uint16_t);
            set_bitmap(num_rows);
            ++num_rows;
        }
    }

    void insert_null() {
        if (offset_end + data_size + num_rows / 8 + 1 > PAGE_SIZE) [[unlikely]] {
            save_page();
        }
        unset_bitmap(num_rows);
        ++num_rows;
    }

    void finalize() {
        if (num_rows != 0) {
            save_page();
        }
    }
};

namespace Contest {

void* build_context();
void  destroy_context(void*);

ColumnarTable execute(const Plan& plan, void* context);

} // namespace Contest
