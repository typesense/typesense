#pragma once

#include <string>
#include <vector>
#include <iconv.h>
#include <unicode/brkiter.h>

class Tokenizer {
private:
    const std::string& text;
    size_t i;
    const bool keep_separators;
    const bool normalize;
    const bool no_op;

    size_t token_counter = 0;
    iconv_t cd;

    static const size_t CHARS = 0;
    static const size_t SEPARATORS = 1;
    size_t stream_mode;

    std::stringstream out;

    std::string locale;
    icu::BreakIterator* bi = nullptr;
    icu::UnicodeString unicode_text;
    int32_t position = 0;
    int32_t prev_position = -1;

public:

    explicit Tokenizer(const std::string& input,
                       bool keep_separators=true, bool normalize=true, bool no_op=false,
                       const std::string& locale = ""):
            text(input), i(0), keep_separators(keep_separators), normalize(normalize),
            no_op(no_op), locale(locale) {
        cd = iconv_open("ASCII//TRANSLIT", "UTF-8");

        if(!input.empty() && (std::isalnum(text[0]) || (text[i] & ~0x7f) != 0)) {
            // alphanum or non-ascii
            stream_mode = CHARS;
        } else {
            stream_mode = SEPARATORS;
        }

        if(!locale.empty() && locale != "en") {
            UErrorCode status = U_ZERO_ERROR;
            const icu::Locale& icu_locale = icu::Locale(locale.c_str());
            bi = icu::BreakIterator::createWordInstance(icu_locale, status);

            unicode_text = icu::UnicodeString::fromUTF8(text);
            bi->setText(unicode_text);

            position = bi->first();
            prev_position = -1;
        }
    }

    ~Tokenizer() {
        iconv_close(cd);
        delete bi;
    }

    bool next(std::string& token, size_t& token_index);

    void tokenize(std::vector<std::string>& tokens);

    void tokenize(std::string& token);
};