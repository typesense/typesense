#pragma once

#include <string>
#include <vector>
#include <iconv.h>

class Tokenizer {
private:
    const std::string& text;
    size_t i;
    const bool keep_empty;
    const bool normalize;
    const bool no_op;

    size_t token_counter = 0;
    iconv_t cd;

public:

    explicit Tokenizer(const std::string& input,
                       bool keep_empty=true, bool normalize=true, bool no_op=false):
            text(input), i(0), keep_empty(keep_empty), normalize(normalize), no_op(no_op) {
        cd = iconv_open("ASCII//TRANSLIT", "UTF-8");
    }

    ~Tokenizer() {
        iconv_close(cd);
    }

    bool next(std::string& token, size_t& token_index);

    void tokenize(std::vector<std::string>& tokens);

    void tokenize(std::string& token);
};