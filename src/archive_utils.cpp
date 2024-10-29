#include "archive_utils.h"
#include "tsconfig.h"
#include <fstream>
#include <cstdio>
#include <cstring>
#include <stdexcept>

bool ArchiveUtils::extract_tar_gz_from_file(const std::string& archive_path, const std::string& destination_path) {
    struct archive* a = nullptr;
    struct archive* ext = nullptr;
    struct archive_entry* entry;
    int flags = ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM | ARCHIVE_EXTRACT_ACL | ARCHIVE_EXTRACT_FFLAGS;
    bool extraction_successful = true;

    a = archive_read_new();
    if (!a) {
        return false;
    }

    archive_read_support_format_all(a);
    archive_read_support_filter_all(a);

    ext = archive_write_disk_new();
    if (!ext) {
        archive_read_free(a);
        return false;
    }

    archive_write_disk_set_options(ext, flags);
    archive_write_disk_set_standard_lookup(ext);

    if (archive_read_open_filename(a, archive_path.c_str(), BUFFER_SIZE) != ARCHIVE_OK) {
        extraction_successful = false;
    }

    while (extraction_successful) {
        int r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            break;
        }
        if (r < ARCHIVE_WARN) {
            extraction_successful = false;
            break;
        }

        const char* current_file = archive_entry_pathname(entry);
        std::string full_path = destination_path + "/" + current_file;
        archive_entry_set_pathname(entry, full_path.c_str());

        r = archive_write_header(ext, entry);
        if (r < ARCHIVE_OK) {
            extraction_successful = false;
            break;
        }

        if (archive_entry_size(entry) > 0) {
            r = copy_data(a, ext);
            if (r < ARCHIVE_WARN) {
                extraction_successful = false;
                break;
            }
        }

        r = archive_write_finish_entry(ext);
        if (r < ARCHIVE_WARN) {
            extraction_successful = false;
            break;
        }
    }

    archive_read_close(a);
    archive_read_free(a);
    archive_write_close(ext);
    archive_write_free(ext);

    return extraction_successful;
}

bool ArchiveUtils::extract_tar_gz_from_memory(const std::string& archive_content, const std::string& destination_path) {
    if (archive_content.empty()) {
        return false;
    }
    std::string temp_file_path = create_temp_tar_gz(archive_content);
    bool result = extract_tar_gz_from_file(temp_file_path, destination_path);
    cleanup(temp_file_path);
    return result;
}

int ArchiveUtils::copy_data(struct archive* ar, struct archive* aw) {
    int r;
    const void* buff;
    size_t size;
    la_int64_t offset;

    for (;;) {
        r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF)
            return ARCHIVE_OK;
        if (r < ARCHIVE_OK)
            return r;
        r = archive_write_data_block(aw, buff, size, offset);
        if (r < ARCHIVE_OK) {
            return r;
        }
    }
}

bool ArchiveUtils::create_directory(const std::string& path) {
    return mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0;
}

std::string ArchiveUtils::create_temp_tar_gz(const std::string& content) {
    std::string tmp_dir = Config::get_instance().get_data_dir() + "/tmp";
    
    if (!create_directory(tmp_dir)) {
        throw std::runtime_error("Failed to create temporary directory: " + tmp_dir);
    }

    std::string temp_file_template = tmp_dir + "/archive_XXXXXX";
    std::vector<char> temp_filename(temp_file_template.begin(), temp_file_template.end());
    temp_filename.push_back('\0');

    int fd = mkstemp(temp_filename.data());
    if (fd == -1) {
        throw std::runtime_error("Failed to create temporary file");
    }

    std::string temp_file_path = std::string(temp_filename.data()) + TAR_GZ_EXTENSION;
    close(fd);
    
    if (std::rename(temp_filename.data(), temp_file_path.c_str()) != 0) {
        throw std::runtime_error("Failed to rename temporary file");
    }

    std::ofstream temp_file(temp_file_path, std::ios::binary);
    if (!temp_file) {
        throw std::runtime_error("Failed to open temporary file for writing");
    }

    temp_file.write(content.data(), content.size());
    if (!temp_file) {
        throw std::runtime_error("Failed to write content to temporary file");
    }

    return temp_file_path;
}

void ArchiveUtils::cleanup(const std::string& file_path) {
    if (std::remove(file_path.c_str()) != 0) {
        throw std::runtime_error("Failed to delete temporary file: " + file_path);
    }

    // Delete the temp directory
    std::string tmp_dir = Config::get_instance().get_data_dir() + "/tmp";
    if (rmdir(tmp_dir.c_str()) != 0) {
        throw std::runtime_error("Failed to delete temporary directory: " + tmp_dir);
    }
}

bool ArchiveUtils::verify_tar_gz_archive(const std::string& archive_content) {
    struct archive* a = archive_read_new();
    if (!a) {
        return false;
    }

    bool is_valid = true;

    archive_read_support_format_all(a);
    archive_read_support_filter_all(a);

    if (archive_read_open_memory(a, archive_content.data(), archive_content.size()) != ARCHIVE_OK) {
        is_valid = false;
    }

    struct archive_entry* entry;
    while (is_valid) {
        int r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            break;
        }
        if (r < ARCHIVE_WARN) {
            is_valid = false;
            break;
        }
    }

    archive_read_close(a);
    archive_read_free(a);

    return is_valid;
}
