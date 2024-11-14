#pragma once

#include <string>
#include <vector>
#include <archive.h>
#include <archive_entry.h>
#include <fcntl.h>
#include <sys/stat.h>

class ArchiveUtils {
public:
    static constexpr const char* TAR_GZ_EXTENSION = ".tar.gz";
    static bool extract_tar_gz_from_file(const std::string& archive_path, const std::string& destination_path);
    static bool extract_tar_gz_from_memory(const std::string& archive_content, const std::string& destination_path);
    static bool verify_tar_gz_archive(const std::string& archive_content);
private:
    static constexpr size_t BUFFER_SIZE = (10 * 1024 * 1024);

    static int copy_data(struct archive* ar, struct archive* aw);
    static bool create_directory(const std::string& path);
    static std::string create_temp_tar_gz(const std::string& content);
    static void cleanup(const std::string& file_path);
};
