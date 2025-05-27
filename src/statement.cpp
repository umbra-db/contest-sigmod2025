#include <cassert>

#include <common.h>
#include <inner_column.h>
#include <plan.h>
#include <statement.h>

std::vector<uint8_t> bitmap_not(std::vector<uint8_t> bitmap) {
    auto task = [&bitmap](size_t begin, size_t end) {
        for (size_t i = begin; i < end; ++i) {
            bitmap[i] = ~bitmap[i];
        }
    };
    filter_tp.run(task, bitmap.size());
    return bitmap;
}

std::vector<uint8_t> bitmap_and(const std::vector<uint8_t>& lhs,
    const std::vector<uint8_t>&                             rhs) {
    std::vector<uint8_t> ret;
    assert(lhs.size() == rhs.size());
    ret.resize(lhs.size());
    auto task = [&lhs, &rhs, &ret](size_t begin, size_t end) {
        for (size_t i = begin; i < end; ++i) {
            ret[i] = lhs[i] & rhs[i];
        }
    };
    filter_tp.run(task, lhs.size());
    return ret;
}

std::vector<uint8_t> bitmap_or(const std::vector<uint8_t>& lhs,
    const std::vector<uint8_t>&                            rhs) {
    std::vector<uint8_t> ret;
    assert(lhs.size() == rhs.size());
    ret.resize(lhs.size());
    auto task = [&lhs, &rhs, &ret](size_t begin, size_t end) {
        for (size_t i = begin; i < end; ++i) {
            ret[i] = lhs[i] | rhs[i];
        }
    };
    filter_tp.run(task, lhs.size());
    return ret;
}

std::vector<uint8_t> Comparison::eval(const std::vector<const InnerColumnBase*>& table) const {
    auto* c = table[column];
    switch (c->type) {
    case DataType::INT32: {
        auto column = reinterpret_cast<const InnerColumn<int32_t>*>(c);
        if (op == IS_NULL) {
            return bitmap_not(column->bitmap);
        } else if (op == IS_NOT_NULL) {
            return column->bitmap;
        } else {
            auto comp_value = static_cast<int32_t>(std::get<int64_t>(value));
            switch (op) {
            case EQ:  return column->equal(comp_value);
            case NEQ: return column->not_equal(comp_value);
            case LT:  return column->less(comp_value);
            case GT:  return column->greater(comp_value);
            case LEQ: return column->less_equal(comp_value);
            case GEQ: return column->greater_equal(comp_value);
            default:  unreachable();
            }
        }
        break;
    }
    case DataType::INT64: {
        auto column = reinterpret_cast<const InnerColumn<int64_t>*>(c);
        if (op == IS_NULL) {
            return bitmap_not(column->bitmap);
        } else if (op == IS_NOT_NULL) {
            return column->bitmap;
        } else {
            auto comp_value = std::get<int64_t>(value);
            switch (op) {
            case EQ:  return column->equal(comp_value);
            case NEQ: return column->not_equal(comp_value);
            case LT:  return column->less(comp_value);
            case GT:  return column->greater(comp_value);
            case LEQ: return column->less_equal(comp_value);
            case GEQ: return column->greater_equal(comp_value);
            default:  unreachable();
            }
        }
        break;
    }
    case DataType::FP64: {
        auto column = reinterpret_cast<const InnerColumn<double>*>(c);
        if (op == IS_NULL) {
            return bitmap_not(column->bitmap);
        } else if (op == IS_NOT_NULL) {
            return column->bitmap;
        } else {
            auto comp_value = std::get<double>(value);
            switch (op) {
            case EQ:  return column->equal(comp_value);
            case NEQ: return column->not_equal(comp_value);
            case LT:  return column->less(comp_value);
            case GT:  return column->greater(comp_value);
            case LEQ: return column->less_equal(comp_value);
            case GEQ: return column->greater_equal(comp_value);
            default:  unreachable();
            }
        }
        break;
    }
    case DataType::VARCHAR: {
        auto column = reinterpret_cast<const InnerColumn<std::string>*>(c);
        if (op == IS_NULL) {
            return bitmap_not(column->bitmap);
        } else if (op == IS_NOT_NULL) {
            return column->bitmap;
        } else {
            auto& comp_value = std::get<std::string>(value);
            switch (op) {
            case EQ:          return column->equal(comp_value);
            case NEQ:         return column->not_equal(comp_value);
            case LT:          return column->less(comp_value);
            case GT:          return column->greater(comp_value);
            case LEQ:         return column->less_equal(comp_value);
            case GEQ:         return column->greater_equal(comp_value);
            case LIKE:        return column->like(comp_value);
            case NOT_LIKE:    return column->not_like(comp_value);
            default:          unreachable();
            }
        }
        break;
    }
    }
    unreachable();
}

bool Comparison::eval(const std::vector<Data>& record) const {
    const Data& record_data = record[column];
    const auto& comp_value  = value;

    switch (op) {
    case IS_NULL:     return std::holds_alternative<std::monostate>(record_data);
    case IS_NOT_NULL: return !std::holds_alternative<std::monostate>(record_data);
    default:          break;
    }

    if (op == LIKE || op == NOT_LIKE) {
        const std::string* record_str = std::get_if<std::string>(&record_data);
        const std::string* comp_str   = std::get_if<std::string>(&comp_value);
        if (!record_str || !comp_str) {
            return false;
        }
        bool match = like_match(*record_str, *comp_str);
        return (op == LIKE) ? match : !match;
    } else {
        auto record_num = get_numeric_value(record_data);
        auto comp_num   = get_numeric_value(comp_value);
        if (record_num.has_value() && comp_num.has_value()) {
            switch (op) {
            case EQ:  return *record_num == *comp_num;
            case NEQ: return *record_num != *comp_num;
            case LT:  return *record_num < *comp_num;
            case GT:  return *record_num > *comp_num;
            case LEQ: return *record_num <= *comp_num;
            case GEQ: return *record_num >= *comp_num;
            default:  return false;
            }
        } else {
            const std::string* record_str = std::get_if<std::string>(&record_data);
            const std::string* comp_str   = std::get_if<std::string>(&comp_value);
            if (record_str && comp_str) {
                switch (op) {
                case EQ:  return *record_str == *comp_str;
                case NEQ: return *record_str != *comp_str;
                case LT:  return *record_str < *comp_str;
                case GT:  return *record_str > *comp_str;
                case LEQ: return *record_str <= *comp_str;
                case GEQ: return *record_str >= *comp_str;
                default:  return false;
                }
            } else {
                return false;
            }
        }
    }
}

std::vector<uint8_t> LogicalOperation::eval(
    const std::vector<const InnerColumnBase*>& table) const {
    switch (op_type) {
    case AND: {
        return bitmap_and(children[0]->eval(table), children[1]->eval(table));
    }
    case OR: {
        return bitmap_or(children[0]->eval(table), children[1]->eval(table));
    }
    case NOT: {
        return bitmap_not(children[0]->eval(table));
    }
    }
    unreachable();
}

bool LogicalOperation::eval(const std::vector<Data>& record) const {
    switch (op_type) {
    case AND: {
        for (const auto& child: children) {
            if (!child->eval(record)) {
                return false;
            }
        }
        return true;
    }
    case OR: {
        for (const auto& child: children) {
            if (child->eval(record)) {
                return true;
            }
        }
        return false;
    }
    case NOT: {
        if (children.size() != 1) {
            return false;
        }
        return !children[0]->eval(record);
    }
    default: return false;
    }
}
