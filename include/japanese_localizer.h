#pragma once

#include <string>

class JapaneseLocalizer {
private:
    JapaneseLocalizer();

    ~JapaneseLocalizer() = default;

    static void write_data_file(const std::string& base64_data, const std::string& file_name);

public:

    static JapaneseLocalizer & get_instance() {
        static JapaneseLocalizer instance;
        return instance;
    }

    bool init();

    char* normalize(const std::string& text);
};
