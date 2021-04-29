#include <sstream>
#include "tokenizer.h"

bool Tokenizer::next(std::string &token, size_t& token_index, size_t& start_index, size_t& end_index) {
    if(no_op) {
        if(i == text.size()) {
            return false;
        }

        token = text;
        i = text.size();
        start_index = 0;
        end_index = text.size() - 1;
        return true;
    }

    if(!locale.empty() && locale != "en") {
        while (position != icu::BreakIterator::DONE) {
            //LOG(INFO) << "Position: " << position;
            bool found_token = false;

            if(prev_position != -1) {
                std::string word;
                size_t length = position - prev_position;
                token = unicode_text.tempSubString(prev_position, length).toUTF8String(word);

                //LOG(INFO) << "token: " << token;

                if(!token.empty()) {
                    if (!std::isalnum(token[0]) && is_ascii_char(token[i])) {
                        // ignore ascii symbols
                        found_token = false;
                    } else if(locale == "ko" && token == "·") {
                        found_token = false;
                    } else {
                        found_token = true;
                        token_index = token_counter++;
                    }

                    start_index = utf8_start_index;
                    end_index = utf8_start_index + token.size() - 1;
                    utf8_start_index = end_index + 1;
                }
            }

            prev_position = position;
            position = bi->next();

            if(found_token) {
                return true;
            }
        }

        return false;
    }

    while(i < text.size()) {
        if(is_ascii_char(text[i])) {
            size_t this_stream_mode = get_stream_mode(text[i]);

            if(this_stream_mode == SKIP) {
                i++;
                continue;
            }

            if(this_stream_mode == SEPARATE) {
                if(out.empty()) {
                    i++;
                    continue;
                }

                token = out;
                out.clear();

                token_index = token_counter++;
                end_index = i - 1;
                i++;
                return true;
            } else {
                if(out.empty()) {
                    start_index = i;
                }

                out += normalize ? char(std::tolower(text[i])) : text[i];
                i++;
                continue;
            }
        }

        if(out.empty()) {
            start_index = i;
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
            out += inbuf;
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
            out += inbuf;
        } else {
            for(size_t out_index=0; out_index<5; out_index++) {
                if(!normalize) {
                    out += outbuf[out_index];
                    continue;
                }

                bool unicode_is_ascii = is_ascii_char(outbuf[out_index]);
                bool keep_char = !unicode_is_ascii || std::isalnum(outbuf[out_index]);

                if(keep_char) {
                    if(unicode_is_ascii && std::isalnum(outbuf[out_index])) {
                        outbuf[out_index] = char(std::tolower(outbuf[out_index]));
                    }
                    out += outbuf[out_index];
                }
            }
        }
    }

    token = out;
    out.clear();
    end_index = i - 1;

    if(token.empty()) {
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

bool Tokenizer::next(std::string &token, size_t &token_index) {
    size_t start_index, end_index;
    return next(token, token_index, start_index, end_index);
}
