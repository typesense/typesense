
#include <butil/file_util.h>
#include <butil/files/file_enumerator.h>
#include <butil/string_printf.h>
#include <file_utils.h>

bool directory_exists(const std::string& dir_path) {
    struct stat info;
    return stat(dir_path.c_str(), &info) == 0 && (info.st_mode & S_IFDIR);
}

bool create_directory(const std::string& dir_path) {
    return butil::CreateDirectory(butil::FilePath(dir_path));
}

bool file_exists(const std::string & file_path) {
    struct stat info;
    return stat(file_path.c_str(), &info) == 0 && !(info.st_mode & S_IFDIR);
}

// tries to hard link first
bool copy_dir(const std::string& from_path, const std::string& to_path) {
    struct stat from_stat;

    if (stat(from_path.c_str(), &from_stat) < 0 || !S_ISDIR(from_stat.st_mode)) {
        LOG(WARNING) << "stat " << from_path << " failed";
        return false;
    }

    if (!butil::CreateDirectory(butil::FilePath(to_path))) {
        LOG(WARNING) << "CreateDirectory " << to_path << " failed";
        return false;
    }

    butil::FileEnumerator dir_enum(butil::FilePath(from_path),false, butil::FileEnumerator::FILES);
    for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
        std::string src_file(from_path);
        std::string dst_file(to_path);
        butil::string_appendf(&src_file, "/%s", name.BaseName().value().c_str());
        butil::string_appendf(&dst_file, "/%s", name.BaseName().value().c_str());

        if (0 != link(src_file.c_str(), dst_file.c_str())) {
            if (!butil::CopyFile(butil::FilePath(src_file), butil::FilePath(dst_file))) {
                LOG(WARNING) << "copy " << src_file << " to " << dst_file << " failed";
                return false;
            }
        }
    }

    return true;
}

bool mv_dir(const std::string& from_path, const std::string& to_path) {
    struct stat from_stat;

    if (stat(from_path.c_str(), &from_stat) < 0 || !S_ISDIR(from_stat.st_mode)) {
        LOG(WARNING) << "stat " << from_path << " failed";
        return false;
    }

    if (!butil::CreateDirectory(butil::FilePath(to_path))) {
        LOG(WARNING) << "CreateDirectory " << to_path << " failed";
        return false;
    }

    butil::FileEnumerator file_enum(butil::FilePath(from_path), false, butil::FileEnumerator::FILES
                                                                      | butil::FileEnumerator::DIRECTORIES);
    for (butil::FilePath name = file_enum.Next(); !name.empty(); name = file_enum.Next()) {
        std::string src_file(from_path);
        std::string dst_file(to_path);

        if(name.value() == to_path) {
            // handle edge case when moving a directory into a subdirectory
            continue;
        }

        butil::string_appendf(&src_file, "/%s", name.BaseName().value().c_str());
        butil::string_appendf(&dst_file, "/%s", name.BaseName().value().c_str());

        butil::File::Error error;
        if (!butil::ReplaceFile(butil::FilePath(src_file), butil::FilePath(dst_file), &error)) {
            LOG(WARNING) << "move " << src_file << " to " << dst_file << " failed: " << error;
            return false;
        }
    }

    return true;
}

bool rename_path(const std::string& from_path, const std::string& to_path) {
    return butil::Move(butil::FilePath(from_path), butil::FilePath(to_path));
}

bool delete_path(const std::string& path, bool recursive) {
    return butil::DeleteFile(butil::FilePath(path), recursive);
}

bool dir_enum_count(const std::string &path) {
    size_t count = 0;
    butil::FileEnumerator file_enum(butil::FilePath(path), false, butil::FileEnumerator::FILES
                                                                       | butil::FileEnumerator::DIRECTORIES);
    for (butil::FilePath name = file_enum.Next(); !name.empty(); name = file_enum.Next()) {
        count++;
    }

    return count;
}
