#pragma once

#include <string>
#include <vector>
#include <iconv.h>
#include <unicode/brkiter.h>
#include <unicode/normalizer2.h>
#include "japanese_localizer.h"
#include "logger.h"

class Tokenizer {
private:
    std::string_view text;
    size_t i;
    const bool normalize;
    const bool no_op;

    size_t token_counter = 0;
    iconv_t cd;

    static const size_t INDEX = 0;
    static const size_t SEPARATE = 1;
    static const size_t SKIP = 2;

    uint8_t index_symbols[256] = {};

    std::string out;

    std::string locale;
    icu::BreakIterator* bi = nullptr;
    icu::UnicodeString unicode_text;
    int32_t position = 0;
    int32_t prev_position = -1;
    int32_t utf8_start_index = 0;
    char* normalized_text = nullptr;

    // non-deletable singleton
    const icu::Normalizer2* nfkd;

    inline size_t get_stream_mode(char c) {
        return (std::isalnum(c) || index_symbols[uint8_t(c)] == 1) ? INDEX : (
            (c == ' ' || c == '\n') ? SEPARATE : SKIP
        );
    }

    static inline bool is_ascii_char(char c) {
        return (c & ~0x7f) == 0;
    }

public:

    explicit Tokenizer(const std::string& input,
                       bool normalize=true, bool no_op=false,
                       const std::string& locale = "",
                       const std::vector<char>& symbols_to_index = {});

    ~Tokenizer() {
        iconv_close(cd);
        free(normalized_text);
        delete bi;
    }

    bool next(std::string& token, size_t& token_index, size_t& start_index, size_t& end_index);

    bool next(std::string& token, size_t& token_index);

    void tokenize(std::vector<std::string>& tokens);

    void tokenize(std::string& token);
};