#pragma once

#include <vector>

#include <cstdlib>

class CSVParser {
public:
    enum Error {
        Ok,
        QuoteNotClosed,
        InconsistentColumns,
        NoTrailingComma,
    };

    CSVParser(char escape = '"', char sep = ',', bool has_trailing_comma = false)
    : escape_(escape)
    , comma_(sep)
    , has_trailing_comma_(has_trailing_comma) {}

    [[nodiscard]] Error execute(const char* buffer, size_t len);
    [[nodiscard]] Error finish();

    virtual void on_field(size_t col_idx, size_t row_idx, const char* begin, size_t len) = 0;

private:
    // configure
    char escape_{'"'}; // may also be '\\'
    char comma_{','};  // may also be '|'
    // true means # commas = # columns and the last comma in each line is followed by the record
    // seperator; false means # commas + 1 = # columns
    bool has_trailing_comma_{false};

    // states
    std::vector<char> current_field_;
    size_t            col_idx_{0};
    size_t            row_idx_{0};
    size_t            num_cols_{0};
    bool              after_first_row_{false};
    bool              quoted_{false};
    bool              after_field_sep_{false};
    bool              after_record_sep_{false};
    bool              escaping_{false};
    bool              newlining_{false};
};
