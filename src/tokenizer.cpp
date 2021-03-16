#include <sstream>
#include "tokenizer.h"

bool Tokenizer::next(std::string &token, size_t& token_index) {
    std::stringstream out;

    if(i >= text.size()) {
        if(i == text.size() && !text.empty() && text.back() == ' ') {
            token = "";
            i++;
            return true;
        }

        return false;
    }

    if(no_op) {
        token = text;
        i = text.size();
        return true;
    }

    while(i < text.size()) {
        if((text[i] & ~0x7f) == 0 ) {
            // ASCII character: split on space/newline or lowercase otherwise
            if(std::isalnum(text[i])) {
                if(normalize) {
                    out << char(std::tolower(text[i]));
                } else {
                    out << text[i];
                }
            } else {
                bool is_space = text[i] == 32;
                bool is_new_line = text[i] == 10;
                bool is_whitespace = is_space || is_new_line;

                bool next_char_alphanum = (i != text.length() - 1) && std::isalnum(text[i + 1]);

                if(!normalize && !is_whitespace && (i == text.length() - 1 || !next_char_alphanum)) {
                    // checking for next char ensures that `foo-bar` does not get split to `foo-`
                    out << text[i];
                }

                if(is_whitespace || next_char_alphanum) {
                    // we split on space or on a special character whose next char is alphanumeric
                    token = out.str();
                    out.clear();
                    i++;

                    if(!keep_empty && token.empty()) {
                        continue;
                    }

                    token_index = token_counter++;
                    return true;
                }
            }

            i++;
            continue;
        }

        char inbuf[5];
        char *p = inbuf;

        // group bytes to form a unicode representation
        *p++ = text[i++];
        if ((text[i] & 0xC0) == 0x80) *p++ = text[i++];
        if ((text[i] & 0xC0) == 0x80) *p++ = text[i++];
        if ((text[i] & 0xC0) == 0x80) *p++ = text[i++];
        *p = 0;
        size_t insize = (p - &inbuf[0]);

        if(!normalize) {
            out << inbuf;
            continue;
        }

        char outbuf[5] = {};
        size_t outsize = sizeof(outbuf);
        char *outptr = outbuf;
        char *inptr = inbuf;

        //printf("[%s]\n", inbuf);

        errno = 0;
        iconv(cd, &inptr, &insize, &outptr, &outsize);

        if(errno == EILSEQ) {
            // symbol cannot be represented as ASCII, so write the original symbol
            out << inbuf;
        } else {
            // NOTE: outsize indicates bytes available AFTER current position so have to do <=
            for(size_t out_index=0; out_index<5; out_index++) {
                if(!normalize) {
                    out << outbuf[out_index];
                    continue;
                }

                bool is_ascii = ((outbuf[out_index] & ~0x7f) == 0);
                bool keep_char = !is_ascii || std::isalnum(outbuf[out_index]);

                if(keep_char) {
                    if(is_ascii && std::isalnum(outbuf[out_index])) {
                        outbuf[out_index] = char(std::tolower(outbuf[out_index]));
                    }
                    out << outbuf[out_index];
                }
            }
        }
    }

    token = out.str();
    out.clear();

    if(!keep_empty && token.empty()) {
        return false;
    }

    token_index = token_counter++;
    return true;
}

void Tokenizer::tokenize(std::vector<std::string> &tokens) {
    std::string token;
    size_t token_index;

    while(next(token, token_index)) {
        tokens.push_back(token);
    }
}

void Tokenizer::tokenize(std::string& token) {
    size_t token_index;
    next(token, token_index);
}
