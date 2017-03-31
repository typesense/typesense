#pragma once

#include <string>
#include <algorithm>
#include <sstream>

struct StringUtils {

    template<class ContainerT>
    static void tokenize(const std::string &str, ContainerT &tokens,
                  const std::string &delimiters = " ", bool trimEmpty = true, unsigned long maxTokenLength = 100) {
        const std::string truncated_str = str.substr(0, maxTokenLength);
        std::string::size_type pos, lastPos = 0;

        using value_type = typename ContainerT::value_type;
        using size_type  = typename ContainerT::size_type;

        while (true) {
            pos = truncated_str.find_first_of(delimiters, lastPos);
            if (pos == std::string::npos) {
                pos = truncated_str.length();

                if (pos != lastPos || !trimEmpty)
                    tokens.push_back(value_type(truncated_str.data() + lastPos,
                                                (size_type) pos - lastPos));

                break;
            }
            else {
                if (pos != lastPos || !trimEmpty)
                    tokens.push_back(value_type(truncated_str.data() + lastPos,
                                                (size_type) pos - lastPos));
            }

            lastPos = pos + 1;
        }
    }

    static std::string replace_all(std::string str, const std::string &from, const std::string &to) {
        size_t start_pos = 0;
        while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
            str.replace(start_pos, from.length(), to);
            start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
        }
        return str;
    }

    // Adapted from: http://stackoverflow.com/a/236180/131050
    static void split(const std::string& s, std::vector<std::string> & result, const std::string& delim, const bool keep_empty = false) {
        if (delim.empty()) {
            result.push_back(s);
            return ;
        }
        std::string::const_iterator substart = s.begin(), subend;
        while (true) {
            subend = std::search(substart, s.end(), delim.begin(), delim.end());
            std::string temp(substart, subend);
            temp = trim(temp);

            if (keep_empty || !temp.empty()) {
                result.push_back(temp);
            }
            if (subend == s.end()) {
                break;
            }
            substart = subend + delim.size();
        }
    }

    // Adapted from: http://stackoverflow.com/a/36000453/131050
    static std::string & trim(std::string & str) {
        // right trim
        while (str.length () > 0 && (str [str.length ()-1] == ' ')) {
            str.erase (str.length ()-1, 1);
        }

        // left trim
        while (str.length () > 0 && (str [0] == ' ')) {
            str.erase (0, 1);
        }

        return str;
    }

    // URL decoding - adapted from: http://stackoverflow.com/a/32595923/131050

    static char from_hex(char ch) {
        return isdigit(ch) ? ch - '0' : tolower(ch) - 'a' + 10;
    }

    static std::string url_decode(std::string text) {
        char h;
        std::ostringstream escaped;
        escaped.fill('0');

        for (auto i = text.begin(), n = text.end(); i != n; ++i) {
            std::string::value_type c = (*i);

            if (c == '%') {
                if (i[1] && i[2]) {
                    h = from_hex(i[1]) << 4 | from_hex(i[2]);
                    escaped << h;
                    i += 2;
                }
            } else if (c == '+') {
                escaped << ' ';
            } else {
                escaped << c;
            }
        }

        return escaped.str();
    }

    // Adapted from: http://stackoverflow.com/a/2845275/131050
    static bool is_integer(const std::string &s) {
        if(s.empty() || ((!isdigit(s[0])) && (s[0] != '-') && (s[0] != '+'))) {
            return false;
        }

        char * p ;
        strtol(s.c_str(), &p, 10);
        return (*p == 0);
    }
};