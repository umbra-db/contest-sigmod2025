#include "query/QueryPlan.hpp"
#include "infra/helper/Misc.hpp"
#include "infra/Scheduler.hpp"
#include "infra/Util.hpp"
#include <cstring>
#include <exception>
#include <fstream>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
struct DataSource::Header {
    static constexpr uint64_t markerValue = []() {
        uint64_t marker = 0;
        std::string_view txt("s1gmod25");
        for (size_t i = 0; i < txt.size(); ++i)
            marker |= uint64_t(txt[i]) << (i * 8);
        return marker;
    }();
    uint64_t marker;
    uint64_t numTables;
};
//---------------------------------------------------------------------------
static_assert(sizeof(DataSource::Header) < PAGE_SIZE);
//---------------------------------------------------------------------------
struct DataSource::TableHeader {
    uint64_t numRows;
    uint64_t numColumns;
    uint64_t nameLen;
    char name[DataSource::Table::nameLenLimit];
};
//---------------------------------------------------------------------------
struct DataSource::ColumnHeader {
    uint64_t dataType;
    uint64_t pageStart;
    uint64_t pageEnd;
};
//---------------------------------------------------------------------------
// 2MB should be enough for anybody
static constexpr size_t HEADER_SIZE = 1ull << 21;
static_assert(HEADER_SIZE % PAGE_SIZE == 0);
//---------------------------------------------------------------------------
std::string DataSource::Table::fixName(std::string name) {
    if (name.size() <= nameLenLimit)
        return std::move(name);
    name.resize(nameLenLimit - 30);
    assert(name.size() <= nameLenLimit);
    return std::move(name);
}
//---------------------------------------------------------------------------
void DataSource::serialize(const std::string& filename) && {
    std::string targetFile = filename + ".tmp";
    auto header = std::make_unique<uint64_t[]>(HEADER_SIZE / sizeof(uint64_t));

    uint64_t* cur = header.get();
    auto write = [&](const auto& data) {
        assert(sizeof(data) % sizeof(uint64_t) == 0);
        if (cur + sizeof(data) / sizeof(uint64_t) > header.get() + HEADER_SIZE / sizeof(uint64_t))
            throw std::runtime_error("Header too large.");
        memcpy(cur, &data, sizeof(data));
        cur += sizeof(data) / sizeof(uint64_t);
    };

    uint64_t curPage = HEADER_SIZE;
    write(Header{Header::markerValue, relations.size()});
    for (const auto& table : relations) {
        TableHeader header{table.numRows, table.columns.size(), table.name.size()};
        memset(header.name, 0, sizeof(header.name));
        assert(table.name.size() < sizeof(header.name));
        memcpy(header.name, table.name.data(), table.name.size());
        write(header);
        for (const auto& column : table.columns) {
            write(ColumnHeader{static_cast<uint64_t>(column.type), curPage, curPage + column.pages.size() * PAGE_SIZE});
            curPage += column.pages.size() * PAGE_SIZE;
        }
    }

    std::ofstream file(targetFile, std::ios::binary);
    file.write(reinterpret_cast<const char*>(header.get()), HEADER_SIZE);
    for (const auto& table : relations) {
        for (const auto& column : table.columns) {
            for (const auto* page : column.pages) {
                file.write(reinterpret_cast<const char*>(page), PAGE_SIZE);
            }
        }
    }

    if (!file)
        throw std::runtime_error("Failed to write file");

    file.close();
    fileMapping.reset();
    relations.clear();
    if (rename(targetFile.c_str(), filename.c_str()) != 0)
        throw std::runtime_error("Failed to rename temp file to correct destination");
}
//---------------------------------------------------------------------------
DataSource DataSource::deserialize(const std::string& filename) {
    using namespace std;
    DataSource result;
    result.fileMapping = Mmap::mapFile(filename);
    if (!result.fileMapping) {
        throw std::runtime_error("Failed to open file");
    }

    auto* cur = reinterpret_cast<const uint64_t*>(result.fileMapping.data());
    auto* headerEnd = cur + HEADER_SIZE / sizeof(uint64_t);
    auto read = [&]<typename T>(engine::type_identity<T>) -> T {
        if (cur + sizeof(T) / sizeof(uint64_t) > headerEnd) {
            throw std::runtime_error("File is corrupted, header too large");
        }
        T data;
        memcpy(&data, cur, sizeof(T));
        cur += sizeof(T) / sizeof(uint64_t);
        return data;
    };

    // Read and validate header.
    auto header = read(engine::type_identity<Header>());
    if (header.marker != Header::markerValue) {
        throw std::runtime_error("File is corrupted, invalid header marker");
    }

    result.relations.reserve(header.numTables);
    // Deserialize each table.
    for (uint64_t i = 0; i < header.numTables; ++i) {
        auto tableHeader = read(engine::type_identity<TableHeader>());
        Table table;
        table.columns.reserve(tableHeader.numColumns);
        table.numRows = tableHeader.numRows;
        table.name = std::string(tableHeader.name, tableHeader.nameLen);
        // Deserialize each column in the table.
        for (uint64_t j = 0; j < tableHeader.numColumns; ++j) {
            auto columnHeader = read(engine::type_identity<ColumnHeader>());
            Column column;
            column.type = static_cast<DataType>(columnHeader.dataType);
            // Determine number of pages for this column.
            if (columnHeader.pageStart % PAGE_SIZE != 0 || columnHeader.pageEnd % PAGE_SIZE != 0) {
                throw std::runtime_error("File is corrupted, invalid page start/end");
            }
            if (columnHeader.pageStart > columnHeader.pageEnd) {
                throw std::runtime_error("File is corrupted, page start comes after end");
            }
            if (columnHeader.pageEnd > result.fileMapping.size()) {
                throw std::runtime_error("File is corrupted, page end out of bounds");
            }
            uint64_t curPage = columnHeader.pageStart;
            column.pagesStorage.reserve((columnHeader.pageEnd - columnHeader.pageStart) / PAGE_SIZE);
            while (curPage < columnHeader.pageEnd) {
                column.pagesStorage.push_back(reinterpret_cast<Page*>(result.fileMapping.data() + curPage));
                curPage += PAGE_SIZE;
            }
            column.pages = column.pagesStorage;
            table.columns.push_back(std::move(column));
        }
        result.relations.push_back(std::move(table));
    }

    return result;
}
//---------------------------------------------------------------------------
}
