#include "string_utils.h"
#include <iostream>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <random>
#include <openssl/sha.h>
#include <map>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "logger.h"

StringUtils::StringUtils() {
    UErrorCode errcode = U_ZERO_ERROR;
    nfkd = icu::Normalizer2::getNFKDInstance(errcode);
}

StringUtils::~StringUtils() {

}

std::string lower_and_no_special_chars(const std::string & str) {
    std::stringstream ss;

    for(char c : str) {
        bool is_ascii = ((int)(c) >= 0);
        bool keep_char = !is_ascii || std::isalnum(c);

        if(keep_char) {
            ss << (char) std::tolower(c);
        }
    }

    return ss.str();
}

std::string StringUtils::randstring(size_t length) {
    static auto& chrs = "0123456789"
                        "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    thread_local std::mt19937 rg(std::random_device{}());
    thread_local std::uniform_int_distribution<uint32_t> pick(0, sizeof(chrs) - 2);

    std::string s;
    s.reserve(length);

    while(length--) {
        s += chrs[pick(rg)];
    }

    return s;
}

std::string StringUtils::hmac(const std::string& key, const std::string& msg) {
    unsigned int hmac_len;
    unsigned char hmac[EVP_MAX_MD_SIZE];
    HMAC(EVP_sha256(), key.c_str(), key.size(),
         reinterpret_cast<const unsigned char *>(msg.c_str()), msg.size(),
         hmac, &hmac_len);

    std::string digest_raw(reinterpret_cast<char*>(&hmac), hmac_len);
    return StringUtils::base64_encode(digest_raw);
}

std::string StringUtils::str2hex(const std::string &str, bool capital) {
    std::string hexstr;
    hexstr.resize(str.size() * 2);
    const size_t a = capital ? 'A' - 1 : 'a' - 1;
    for (size_t i = 0, c = str[0] & 0xFF; i < hexstr.size(); c = str[i / 2] & 0xFF) {
        hexstr[i++] = c > 0x9F ? (c / 16 - 9) | a : c / 16 | '0';
        hexstr[i++] = (c & 0xF) > 9 ? (c % 16 - 9) | a : c % 16 | '0';
    }

    return hexstr;
}

std::string StringUtils::hash_sha256(const std::string& str) {
    const size_t SHA256_SIZE = 32;
    unsigned char hash_buf[SHA256_SIZE];
    SHA256(reinterpret_cast<const unsigned char *>(str.c_str()), str.size(), hash_buf);
    return StringUtils::str2hex(std::string(reinterpret_cast<char*>(hash_buf), SHA256_SIZE));
}

void StringUtils::parse_query_string(const std::string& query, std::map<std::string, std::string>& query_map) {
    std::string key_value;

    int query_len = int(query.size());
    int i = 0;

    if(query[0] == '?') {
        i++;
    }

    while(i < query_len) {
        // we have to support un-encoded "&&" in the query string value, which makes things a bit more complex
        bool start_of_new_param = query[i] == '&' &&
                                  (i != query_len - 1 && query[i + 1] != '&') &&
                                  (i != 0 && query[i - 1] != '&');
        bool end_of_params = (i == query_len - 1);

        if(start_of_new_param || end_of_params) {
            // Save accumulated key_value
            if(end_of_params && query[i] != '&') {
                key_value += query[i];
            }

            size_t j = 0;
            bool iterating_on_key = true;
            std::string key;
            std::string value;

            while(j < key_value.size()) {
                if(key_value[j] == '=' && iterating_on_key) {
                    iterating_on_key = false;
                } else if(iterating_on_key) {
                    key += key_value[j];
                } else {
                    value += key_value[j];
                }

                j++;
            }

            if(!key.empty() && key != "&") {
                value = StringUtils::url_decode(value);

                if (query_map.count(key) == 0) {
                    query_map[key] = value;
                } else if (key == "filter_by") {
                    query_map[key] = query_map[key] + "&&" + value;
                } else {
                    query_map[key] = value;
                }
            }

            key_value = "";
        } else {
            key_value += query[i];
        }

        i++;
    }
}

void StringUtils::split_to_values(const std::string& vals_str, std::vector<std::string>& filter_values) {
    size_t i = 0;

    bool inside_tick = false;
    std::string buffer;
    buffer.reserve(20);

    while(i < vals_str.size()) {
        char c = vals_str[i];
        bool escaped_tick = (i != 0) && c == '`' && vals_str[i-1] == '\\';

        switch(c) {
            case '`':
                if(escaped_tick) {
                    buffer += c;
                } else if(inside_tick && !buffer.empty()) {
                    inside_tick = false;
                } else {
                    inside_tick = true;
                }
                break;
            case ',':
                if(!inside_tick) {
                    filter_values.push_back(StringUtils::trim(buffer));
                    buffer = "";
                } else {
                    buffer += c;
                }
                break;
            default:
                buffer += c;
        }

        i++;
    }

    if(!buffer.empty()) {
        filter_values.push_back(StringUtils::trim(buffer));
    }
}

std::string StringUtils::float_to_str(float value) {
    std::ostringstream os;
    os << value;
    return os.str();
}

std::string StringUtils::unicode_nfkd(const std::string& text) {
    UErrorCode errcode = U_ZERO_ERROR;
    icu::UnicodeString src = icu::UnicodeString::fromUTF8(text);
    icu::UnicodeString dst;
    nfkd->normalize(src, dst, errcode);

    if(!U_FAILURE(errcode)) {
        std::string output;
        dst.toUTF8String(output);
        return output;
    } else {
        LOG(ERROR) << "Unicode error during parsing: " << errcode;
        return text;
    }
}

void StringUtils::replace_all(std::string& subject, const std::string& search, const std::string& replace) {
    if(search.empty()) {
        return ;
    }

    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
        subject.replace(pos, search.length(), replace);
        pos += replace.length();
    }
}

void StringUtils::erase_char(std::string& str, const char c) {
    str.erase(std::remove(str.begin(), str.end(), c), str.cend());
}

std::string StringUtils::trim_curly_spaces(const std::string& str) {
    std::string left_trimmed;
    int i = 0;
    bool inside_curly = false;

    while(i < str.size()) {
        switch (str[i]) {
            case '{':
                left_trimmed += str[i];
                inside_curly = true;
                break;

            case '}':
                left_trimmed += str[i];
                inside_curly = false;
                break;

            case ' ':
                if(!inside_curly) {
                    left_trimmed += str[i];
                    inside_curly = false;
                }
                break;

            default:
                left_trimmed += str[i];
                inside_curly = false;
        }

        i++;
    }

    std::string right_trimmed;
    i = left_trimmed.size()-1;
    inside_curly = false;

    while(i >= 0) {
        switch (left_trimmed[i]) {
            case '}':
                right_trimmed += left_trimmed[i];
                inside_curly = true;
                break;

            case '{':
                right_trimmed += left_trimmed[i];
                inside_curly = false;
                break;

            case ' ':
                if(!inside_curly) {
                    right_trimmed += left_trimmed[i];
                    inside_curly = false;
                }
                break;

            default:
                right_trimmed += left_trimmed[i];
                inside_curly = false;
        }

        i--;
    }

    std::reverse(right_trimmed.begin(), right_trimmed.end());
    return right_trimmed;
}

bool StringUtils::ends_with(const std::string& str, const std::string& ending) {
    if (str.length() >= ending.length()) {
        return (0 == str.compare (str.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}

bool StringUtils::contains_word(const std::string& haystack, const std::string& needle) {
    size_t pos = haystack.find(needle);
    if(pos == std::string::npos) {
        return false;
    }

    if(pos == 0 && haystack.size() == needle.size()) {
        return true;
    }

    if(pos != 0 && haystack[pos - 1] != ' ') {
        return false;
    }

    size_t end_pos = pos + needle.size();
    if(end_pos < haystack.size() and haystack[end_pos] != ' ') {
        return false;
    }

    return true;
}

char* StringUtils::get_ip_str(const struct sockaddr* sa, char* s, size_t maxlen) {
    switch (sa->sa_family) {
        case AF_INET:
            inet_ntop(AF_INET, &(((struct sockaddr_in*) sa)->sin_addr), s, maxlen);
            break;

        case AF_INET6:
            inet_ntop(AF_INET6, &(((struct sockaddr_in6*) sa)->sin6_addr), s, maxlen);
            break;

        default:
            strncpy(s, "Unknown AF", maxlen);
            return NULL;
    }

    return s;
}

size_t StringUtils::get_num_chars(const std::string& s) {
    // finds number of unicode points in given string
    size_t i = 0, j = 0;
    while (s[i]) {
        if ((s[i] & 0xC0) != 0x80) {
            j++;
        }
        i++;
    }

    return j;
}

Option<bool> parse_reference_filter(const std::string& filter_query, std::queue<std::string>& tokens, size_t& index) {
    auto error = Option<bool>(400, "Could not parse the reference filter.");
    if (filter_query[index] != '$') {
        return error;
    }

    int start_index = index;
    auto size = filter_query.size();
    while(++index < size && filter_query[index] != '(') {}

    if (index >= size) {
        return error;
    }

    int parenthesis_count = 1;
    while (++index < size && parenthesis_count > 0) {
        if (filter_query[index] == '(') {
            parenthesis_count++;
        } else if (filter_query[index] == ')') {
            parenthesis_count--;
        }
    }

    if (parenthesis_count != 0) {
        return error;
    }

    tokens.push(filter_query.substr(start_index, index - start_index));
    return Option<bool>(true);
}

Option<bool> StringUtils::tokenize_filter_query(const std::string& filter_query, std::queue<std::string>& tokens) {
    auto size = filter_query.size();
    for (size_t i = 0; i < size;) {
        auto c = filter_query[i];
        if (c == ' ') {
            i++;
            continue;
        }

        if (c == '(') {
            tokens.push("(");
            i++;
        } else if (c == ')') {
            tokens.push(")");
            i++;
        } else if (c == '&') {
            if (i + 1 >= size || filter_query[i + 1] != '&') {
                return Option<bool>(400, "Could not parse the filter filter_query.");
            }
            tokens.push("&&");
            i += 2;
        } else if (c == '|') {
            if (i + 1 >= size || filter_query[i + 1] != '|') {
                return Option<bool>(400, "Could not parse the filter filter_query.");
            }
            tokens.push("||");
            i += 2;
        } else {
            // Reference filter would start with $ symbol.
            if (c == '$') {
                auto op = parse_reference_filter(filter_query, tokens, i);
                if (!op.ok()) {
                    return op;
                }
                continue;
            }

            std::stringstream ss;
            bool inBacktick = false;
            bool preceding_colon = false;
            bool is_geo_value = false;

            do {
                if (c == ':') {
                    preceding_colon = true;
                }
                if (c == ')' && is_geo_value) {
                    is_geo_value = false;
                }

                ss << c;
                c = filter_query[++i];

                if (c == '`') {
                    inBacktick = !inBacktick;
                }
                if (preceding_colon && c == '(') {
                    is_geo_value = true;
                    preceding_colon = false;
                } else if (preceding_colon && c != ' ') {
                    preceding_colon = false;
                }
            } while (i < size && (inBacktick || is_geo_value ||
                                  (c != '(' && c != ')' && !(c == '&' && filter_query[i + 1] == '&') &&
                                   !(c == '|' && filter_query[i + 1] == '|'))));
            auto token = ss.str();
            trim(token);
            tokens.push(token);
        }
    }
    return Option<bool>(true);
}

/*size_t StringUtils::unicode_length(const std::string& bytes) {
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> utf8conv;
    return utf8conv.from_bytes(bytes).size();
}*/
