#include <csv_parser.h>

CSVParser::Error CSVParser::execute(const char* buffer, size_t len) {
    size_t i = 0;
    if (this->escaping_) {
        if (this->escape_ == '"') {
            if (buffer[0] == '"') {
                ++i;
                this->current_field_.push_back('"');
            } else {
                this->quoted_ = false;
            }
        } else {
            char c = buffer[0];
            if (c == '"' or c == this->escape_) {
                this->current_field_.push_back(c);
                ++i;
            } else {
                this->current_field_.push_back(this->escape_);
            }
        }
        this->escaping_ = false;
    }
    if (this->newlining_) {
        if (len > 0 and buffer[0] == '\n') {
            ++i;
        }
        if (this->has_trailing_comma_) {
            if (not this->after_field_sep_) {
                return NoTrailingComma;
            }
            if (not this->after_first_row_) {
                this->after_first_row_ = true;
                this->num_cols_        = this->col_idx_;
            } else [[likely]] {
                if (this->col_idx_ != this->num_cols_) {
                    return InconsistentColumns;
                }
            }
        } else {
            if (not this->after_first_row_) {
                this->after_first_row_ = true;
                this->num_cols_        = this->col_idx_ + 1;
            } else [[likely]] {
                if (this->col_idx_ + 1 != this->num_cols_) {
                    return InconsistentColumns;
                }
            }
            this->on_field(this->col_idx_,
                this->row_idx_,
                this->current_field_.data(),
                this->current_field_.size());
            this->current_field_.clear();
        }
        this->col_idx_ = 0;
        ++this->row_idx_;
        this->after_record_sep_ = true;
        this->newlining_ = false;
    }
    for (; i < len; ++i) {
        bool set_after_record_sep = false;
        bool set_after_field_sep  = false;
        char c                    = buffer[i];
        if (c != this->comma_ and c != '\n' and c != '\r' and c != '"' and c != this->escape_)
            [[likely]] {
            this->current_field_.push_back(c);
        } else if (c == this->comma_) {
            if (not this->quoted_) [[likely]] {
                this->on_field(this->col_idx_,
                    this->row_idx_,
                    this->current_field_.data(),
                    this->current_field_.size());
                this->current_field_.clear();
                ++this->col_idx_;
                set_after_field_sep = true;
            } else {
                this->current_field_.push_back(c);
            }
        } else if (c == '\n' or c == '\r') {
            if (not this->quoted_) [[likely]] {
                if (c == '\r') {
                    if (i + 1 == len) {
                        this->newlining_ = true;
                        return Ok;
                    }
                    if (buffer[i + 1] == '\n') {
                        ++i;
                    }
                }
                if (this->has_trailing_comma_) {
                    if (not this->after_field_sep_) {
                        return NoTrailingComma;
                    }
                    if (not this->after_first_row_) {
                        this->after_first_row_ = true;
                        this->num_cols_        = this->col_idx_;
                    } else [[likely]] {
                        if (this->col_idx_ != this->num_cols_) {
                            return InconsistentColumns;
                        }
                    }
                } else {
                    if (not this->after_first_row_) {
                        this->after_first_row_ = true;
                        this->num_cols_        = this->col_idx_ + 1;
                    } else [[likely]] {
                        if (this->col_idx_ + 1 != this->num_cols_) {
                            return InconsistentColumns;
                        }
                    }
                    this->on_field(this->col_idx_,
                        this->row_idx_,
                        this->current_field_.data(),
                        this->current_field_.size());
                    this->current_field_.clear();
                }
                this->col_idx_ = 0;
                ++this->row_idx_;
                set_after_record_sep = true;
            } else {
                this->current_field_.push_back(c);
            }
        } else if (c == '"') {
            if (this->escape_ == '"') {
                if (not this->quoted_) {
                    this->quoted_ = true;
                } else {
                    if (i + 1 == len) {
                        this->escaping_ = true;
                        return Ok;
                    }
                    if (buffer[i + 1] == '"') {
                        ++i;
                        this->current_field_.push_back(c);
                    } else {
                        this->quoted_ = false;
                    }
                }
            } else {
                this->quoted_ = not this->quoted_;
            }
        } else {
            if (this->quoted_) [[likely]] {
                if (i + 1 == len) {
                    this->escaping_ = true;
                    return Ok;
                }
                char c = buffer[i + 1];
                if (c == '"' or c == this->escape_) {
                    this->current_field_.push_back(c);
                    ++i;
                } else {
                    this->current_field_.push_back(this->escape_);
                }
            } else {
                this->current_field_.push_back(c);
            }
        }
        this->after_field_sep_  = set_after_field_sep;
        this->after_record_sep_ = set_after_record_sep;
    }
    return Ok;
}

CSVParser::Error CSVParser::finish() {
    if (this->quoted_) {
        return QuoteNotClosed;
    } else if (this->newlining_) {
        return this->execute("", 0);
    } else if (not this->after_record_sep_) {
        return this->execute("\n", 1);
    } else {
        return Ok;
    }
}
