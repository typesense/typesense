#pragma once

bool directory_exists(const std::string & dir_path);

bool create_directory(const std::string& dir_path);

bool file_exists(const std::string & file_path);

// tries to hard link first
bool copy_dir(const std::string& from_path, const std::string& to_path);

bool mv_dir(const std::string& from_path, const std::string& to_path);

bool rename_path(const std::string& from_path, const std::string& to_path);

bool delete_path(const std::string& path, bool recursive = true);

bool dir_enum_count(const std::string & path);