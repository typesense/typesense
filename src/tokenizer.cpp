#include <sstream>
#include "tokenizer.h"

bool Tokenizer::next(std::string &token, size_t& token_index) {
    if(no_op) {
        if(i == text.size()) {
            return false;
        }

        token = text;
        i = text.size();
        return true;
    }

    while(i < text.size()) {
        bool is_ascii = (text[i] & ~0x7f) == 0;
        if(is_ascii) {
            const size_t next_stream_mode = std::isalnum(text[i]) ? CHARS : SEPARATORS;

            if(next_stream_mode != stream_mode) {
                // We tokenize when `stream_mode` changes
                token = out.str();

                out.str(std::string());
                if(normalize) {
                    out << char(std::tolower(text[i]));
                } else {
                    out << text[i];
                }
                i++;

                if(stream_mode == SEPARATORS && !keep_separators) {
                    stream_mode = next_stream_mode;
                    continue;
                }

                token_index = token_counter++;
                stream_mode = next_stream_mode;
                return true;
            } else {
                if(normalize) {
                    out << char(std::tolower(text[i]));
                } else {
                    out << text[i];
                }

                i++;
                continue;
            }
        }

        if(stream_mode == SEPARATORS) { // to detect first non-ascii character
            // we will tokenize now and treat the following non-ascii chars as a different token
            stream_mode = CHARS;
            token = out.str();
            out.str(std::string());

            if(keep_separators) {
                token_index = token_counter++;
                return true;
            }
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
            for(size_t out_index=0; out_index<5; out_index++) {
                if(!normalize) {
                    out << outbuf[out_index];
                    continue;
                }

                bool unicode_is_ascii = ((outbuf[out_index] & ~0x7f) == 0);
                bool keep_char = !unicode_is_ascii || std::isalnum(outbuf[out_index]);

                if(keep_char) {
                    if(unicode_is_ascii && std::isalnum(outbuf[out_index])) {
                        outbuf[out_index] = char(std::tolower(outbuf[out_index]));
                    }
                    out << outbuf[out_index];
                }
            }
        }
    }

    token = out.str();
    out.str(std::string());

    if(token.empty()) {
        return false;
    }

    if(!std::isalnum(token[0]) && !keep_separators) {
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
