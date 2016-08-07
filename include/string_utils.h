#pragma once

#include <string>

struct StringUtils {

    template<class ContainerT>
    static void tokenize(const std::string &str, ContainerT &tokens,
                  const std::string &delimiters = " ", bool trimEmpty = false) {
        std::string::size_type pos, lastPos = 0;

        using value_type = typename ContainerT::value_type;
        using size_type  = typename ContainerT::size_type;

        while (true) {
            pos = str.find_first_of(delimiters, lastPos);
            if (pos == std::string::npos) {
                pos = str.length();

                if (pos != lastPos || !trimEmpty)
                    tokens.push_back(value_type(str.data() + lastPos,
                                                (size_type) pos - lastPos));

                break;
            }
            else {
                if (pos != lastPos || !trimEmpty)
                    tokens.push_back(value_type(str.data() + lastPos,
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
};