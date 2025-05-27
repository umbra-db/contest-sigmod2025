#pragma once

#include <array>
#include <string>

#include <fmt/core.h>

enum class DataType {
    INT32,       // 4-byte integer
    INT64,       // 8-byte integer
    FP64,        // 8-byte floating point
    VARCHAR,     // string of arbitary length
};

template <>
struct fmt::formatter<DataType> {
    template <class ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FormatContext>
    auto format(DataType value, FormatContext& ctx) const {
        static std::array<std::string_view, 4> names{
            "INT32",
            "INT64",
            "FP64",
            "VARCHAR",
        };
        return fmt::format_to(ctx.out(), "{}", names[int(value)]);
    }
};

#define DISPATCH_DATA_TYPE(type, TYPE, ...) \
    do {                                    \
        switch (type) {                     \
        case DataType::INT32: {             \
            using TYPE = int32_t;           \
            __VA_ARGS__                     \
            break;                          \
        }                                   \
        case DataType::INT64: {             \
            using TYPE = int64_t;           \
            __VA_ARGS__                     \
            break;                          \
        }                                   \
        case DataType::FP64: {              \
            using TYPE = double;            \
            __VA_ARGS__                     \
            break;                          \
        }                                   \
        case DataType::VARCHAR: {           \
            using TYPE = std::string;       \
            __VA_ARGS__                     \
            break;                          \
        }                                   \
        }                                   \
    } while (0)

struct Attribute {
    DataType    type;
    std::string name;
};