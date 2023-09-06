#include <sstream>
#include <algorithm>
#include <string_utils.h>
#include "tokenizer.h"

Tokenizer::Tokenizer(const std::string& input, bool normalize, bool no_op, const std::string& locale,
                     const std::vector<char>& symbols_to_index,
                     const std::vector<char>& separators):
                     i(0), normalize(normalize), no_op(no_op), locale(locale) {

    for(char c: symbols_to_index) {
        index_symbols[uint8_t(c)] = 1;
    }

    for(char c: separators) {
        separator_symbols[uint8_t(c)] = 1;
    }

    UErrorCode errcode = U_ZERO_ERROR;

    if(locale == "ko") {
        nfkd = icu::Normalizer2::getNFKDInstance(errcode);
    }

    if(locale == "th") {
        nfkc = icu::Normalizer2::getNFKCInstance(errcode);
    }

    cd = iconv_open("ASCII//TRANSLIT", "UTF-8");

    init(input);
}


void Tokenizer::init(const std::string& input) {
    // init() can be called multiple times safely without leaking memory as we check for prior initialization
    if(normalized_text) {
        free(normalized_text);
        normalized_text = nullptr;
    }

    if(locale == "zh") {
        UErrorCode translit_status = U_ZERO_ERROR;
        if(!transliterator) {
            transliterator = icu::Transliterator::createInstance("Traditional-Simplified",
                                                                 UTRANS_FORWARD, translit_status);
        }
        if(U_FAILURE(translit_status)) {
            //LOG(ERROR) << "Unable to create transliteration instance for `zh` locale.";
            transliterator = nullptr;
            text = input;
        } else {
            icu::UnicodeString unicode_input = icu::UnicodeString::fromUTF8(input);
            transliterator->transliterate(unicode_input);
            std::string output;
            unicode_input.toUTF8String(output);
            normalized_text = (char *)malloc(output.size()+1);
            strcpy(normalized_text, output.c_str());
            text = normalized_text;
        }
    }

    else if(locale == "ja") {
        if(normalize) {
            normalized_text = JapaneseLocalizer::get_instance().normalize(input);
            text = normalized_text;
        } else {
            text = input;
        }
    } else if(is_cyrillic(locale)) {
        // init transliterator but will only transliterate during tokenization
        UErrorCode translit_status = U_ZERO_ERROR;
        if(!transliterator) {
            transliterator = icu::Transliterator::createInstance("Any-Latin; Latin-ASCII",
                                                                 UTRANS_FORWARD, translit_status);
        }
        text = input;
    } else {
        text = input;
    }

    if(!locale.empty() && locale != "en") {
        UErrorCode status = U_ZERO_ERROR;
        const icu::Locale& icu_locale = icu::Locale(locale.c_str());
        if(!bi) {
            bi = icu::BreakIterator::createWordInstance(icu_locale, status);
        }

        unicode_text = icu::UnicodeString::fromUTF8(text);
        bi->setText(unicode_text);

        start_pos = bi->first();
        end_pos = bi->next();
        utf8_start_index = 0;
    }
}

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
        while (end_pos != icu::BreakIterator::DONE) {
            //LOG(INFO) << "Position: " << start_pos;
            std::string word;

            if(locale == "ko") {
                UErrorCode errcode = U_ZERO_ERROR;
                icu::UnicodeString src = unicode_text.tempSubStringBetween(start_pos, end_pos);
                icu::UnicodeString dst;
                nfkd->normalize(src, dst, errcode);

                if(!U_FAILURE(errcode)) {
                    dst.toUTF8String(word);
                } else {
                    LOG(ERROR) << "Unicode error during parsing: " << errcode;
                }
            } else if(normalize && is_cyrillic(locale)) {
                auto raw_text = unicode_text.tempSubStringBetween(start_pos, end_pos);
                transliterator->transliterate(raw_text);
                raw_text.toUTF8String(word);
            } else if(normalize && locale == "th") {
                UErrorCode errcode = U_ZERO_ERROR;
                icu::UnicodeString src = unicode_text.tempSubStringBetween(start_pos, end_pos);
                icu::UnicodeString dst;
                nfkc->normalize(src, dst, errcode);
                if(!U_FAILURE(errcode)) {
                    dst.toUTF8String(word);
                } else {
                    LOG(ERROR) << "Unicode error during parsing: " << errcode;
                }
            } else if(normalize && locale == "ja") {
                auto raw_text = unicode_text.tempSubStringBetween(start_pos, end_pos);
                raw_text.toUTF8String(word);
                char* normalized_word = JapaneseLocalizer::get_instance().normalize(word);
                word.assign(normalized_word, strlen(normalized_word));
                free(normalized_word);
            } else {
                unicode_text.tempSubStringBetween(start_pos, end_pos).toUTF8String(word);
            }

            bool emit_token = false;

            // `word` can be either a multi-byte unicode sequence or an ASCII character
            // ASCII character can be either a special character or English alphabet

            if(is_ascii_char(word[0])) {

                if(std::isalnum(word[0])) {
                    // normalize an ascii string and emit word as token
                    std::transform(word.begin(), word.end(), word.begin(),
                                   [](unsigned char c){ return std::tolower(c); });
                    out += word;
                    emit_token = true;
                }

                else {
                    // special character:
                    // a) present in `index_symbols` -> append word to out and continue iteration
                    // b) present in `separator_symbols` -> skip word
                    // c) not present in either -> skip word
                    if(index_symbols[uint8_t(word[0])] == 1) {
                        out += word;
                        emit_token = true;
                    }
                }


            } else {
                if(locale == "zh" && (word == "，" || word == "─" || word == "。")) {
                    emit_token = false;
                } else if(locale == "ko" && word == "·") {
                    emit_token = false;
                } else {
                    emit_token = true;
                    out += word;
                }
            }

            if(emit_token) {
                token = out;
                token_index = token_counter++;
                out.clear();
            }

            start_index = utf8_start_index;
            end_index = utf8_start_index + word.size() - 1;
            utf8_start_index = end_index + 1;

            start_pos = end_pos;
            end_pos = bi->next();

            if(emit_token) {
                return true;
            }
        }

        token = out;
        out.clear();
        start_index = utf8_start_index;
        end_index = text.size() - 1;

        if(token.empty()) {
            return false;
        }

        token_index = token_counter++;
        return true;
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
        iconv(cd, &inptr, &insize, &outptr, &outsize);  // this can be handled by ICU via "Latin-ASCII"

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

bool Tokenizer::tokenize(std::string& token) {
    size_t token_index = 0;
    init(token);
    return next(token, token_index);
}

bool Tokenizer::next(std::string &token, size_t &token_index) {
    size_t start_index = 0, end_index = 0;
    return next(token, token_index, start_index, end_index);
}

bool Tokenizer::is_cyrillic(const std::string& locale) {
    return locale == "el" || locale == "bg" ||
           locale == "ru" || locale == "sr" || locale == "uk" || locale == "be";
}

void Tokenizer::decr_token_counter() {
    if(token_counter > 0) {
        token_counter--;
    }
}

bool Tokenizer::should_skip_char(char c) {
    return is_ascii_char(c) && get_stream_mode(c) != INDEX;
}

std::string Tokenizer::normalize_ascii_no_spaces(const std::string& text) {
    std::string analytics_query = text;
    StringUtils::trim(analytics_query);

    for(size_t i = 0; i < analytics_query.size(); i++) {
        if(is_ascii_char(text[i])) {
            analytics_query[i] = std::tolower(analytics_query[i]);
        }
    }

    return analytics_query;
}
