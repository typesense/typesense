#pragma once

#include <string>
#include <vector>
#include <iconv.h>
#include <unicode/brkiter.h>
#include <unicode/normalizer2.h>
#include <unicode/translit.h>
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
    uint8_t separator_symbols[256] = {};

    std::string out;

    std::string locale;
    icu::BreakIterator* bi = nullptr;
    icu::UnicodeString unicode_text;

    // tracks start of a text segment that can span multiple unicode tokens due to use of custom symbols
    int32_t utf8_start_index = 0;

    // tracks current unicode segment for text extraction
    int32_t start_pos = 0;
    int32_t end_pos = 0;

    char* normalized_text = nullptr;

    // non-deletable singletons
    const icu::Normalizer2* nfkd = nullptr;
    const icu::Normalizer2* nfkc = nullptr;

    icu::Transliterator* transliterator = nullptr;

    inline size_t get_stream_mode(char c) {
        return (std::isalnum(c) || index_symbols[uint8_t(c)] == 1) ? INDEX : (
            (c == ' ' || c == '\n' || separator_symbols[uint8_t(c)] == 1) ? SEPARATE : SKIP
        );
    }

public:

    explicit Tokenizer(const std::string& input,
                       bool normalize=true, bool no_op=false,
                       const std::string& locale = "",
                       const std::vector<char>& symbols_to_index = {},
                       const std::vector<char>& separators = {});

    ~Tokenizer() {
        iconv_close(cd);
        free(normalized_text);
        delete bi;
        delete transliterator;
    }

    void init(const std::string& input);

    bool next(std::string& token, size_t& token_index, size_t& start_index, size_t& end_index);

    bool next(std::string& token, size_t& token_index);

    void tokenize(std::vector<std::string>& tokens);

    bool tokenize(std::string& token);

    static bool is_cyrillic(const std::string& locale);

    static inline bool is_ascii_char(char c) {
        return (c & ~0x7f) == 0;
    }

    void decr_token_counter();
};