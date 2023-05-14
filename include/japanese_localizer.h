#pragma once

#include <string>
#include <mutex>

class JapaneseLocalizer {
private:
    JapaneseLocalizer();

    ~JapaneseLocalizer() = default;

    static void write_data_file(const std::string& base64_data, const std::string& file_name);

    std::mutex m;

public:

    static JapaneseLocalizer & get_instance() {
        static JapaneseLocalizer instance;
        return instance;
    }

    bool init();

    char* normalize(const std::string& text);
};
