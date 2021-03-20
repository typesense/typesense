#pragma once

#include <string>
#include <vector>
#include <iconv.h>

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

public:

    explicit Tokenizer(const std::string& input,
                       bool keep_separators=true, bool normalize=true, bool no_op=false):
            text(input), i(0), keep_separators(keep_separators), normalize(normalize), no_op(no_op) {
        cd = iconv_open("ASCII//TRANSLIT", "UTF-8");

        if(!input.empty() && (std::isalnum(text[0]) || (text[i] & ~0x7f) != 0)) {
            // alphanum or non-ascii
            stream_mode = CHARS;
        } else {
            stream_mode = SEPARATORS;
        }
    }

    ~Tokenizer() {
        iconv_close(cd);
    }

    bool next(std::string& token, size_t& token_index);

    void tokenize(std::vector<std::string>& tokens);

    void tokenize(std::string& token);
};