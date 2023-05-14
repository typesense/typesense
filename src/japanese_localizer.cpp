#include "japanese_localizer.h"
#include "japanese_data.h"
#include "string_utils.h"
#include "logger.h"
#include <cstdlib>
#include <fstream>

extern "C" {
    #include "libkakasi.h"
}

void JapaneseLocalizer::write_data_file(const std::string &base64_data, const std::string &file_name) {
    const std::string& binary_str = StringUtils::base64_decode(base64_data);
    std::ofstream out(file_name, std::ios::out | std::ios::binary);
    out << binary_str;
    out.flush();
    out.close();
}

bool JapaneseLocalizer::init() {
    const std::string kanwa_data_file_path = "/tmp/kanwa.data";
    const std::string itaji_data_file_path = "/tmp/itaji.data";

    write_data_file(JA_DATA::kanwa_dict, kanwa_data_file_path);
    write_data_file(JA_DATA::itaji_dict, itaji_data_file_path);

    setenv("KANWADICTPATH", kanwa_data_file_path.c_str(), true);
    setenv("ITAIJIDICTPATH", itaji_data_file_path.c_str(), true);

    // initialize kakasi datastructures
    std::vector<std::string> arguments = {"./kakasi", "-JH", "-KH", "-s", "-iutf8", "-outf8"};
    std::vector<char*> argv;
    for (const auto& arg : arguments) {
        argv.push_back((char*)arg.data());
    }

    if (kakasi_getopt_argv(argv.size(), argv.data()) != 0) {
        LOG(ERROR) << "Kakasi initialization failed.";
        return false;
    }

    return true;
}

char* JapaneseLocalizer::normalize(const std::string& text) {
    std::unique_lock lk(m);
    return kakasi_do((char *)text.c_str());
}

JapaneseLocalizer::JapaneseLocalizer() {
    init();
}
