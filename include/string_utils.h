#pragma once

#include <string>
#include <algorithm>
#include <sstream>

struct StringUtils {
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

    static void toupper(std::string& str) {
        std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    }
};