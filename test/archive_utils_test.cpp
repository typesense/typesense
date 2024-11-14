#include <gtest/gtest.h>
#include "archive_utils.h"
#include "tsconfig.h"
#include <fstream>
#include <cstdio>
#include <filesystem>

class ArchiveUtilsTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_dir = std::filesystem::temp_directory_path() / "archive_utils_test";
        std::filesystem::create_directory(temp_dir);
        Config::get_instance().set_data_dir(temp_dir.string());
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }

    std::filesystem::path temp_dir;

    // Helper function to create a simple .tar.gz file
    std::string create_test_tar_gz() {
        std::string content = "This is a test file content.";
        std::string filename = (temp_dir / "test.txt").string();
        std::ofstream file(filename);
        file << content;
        file.close();

        std::string archive_name = (temp_dir / "test.tar.gz").string();
        std::string command = "tar -czf " + archive_name + " -C " + temp_dir.string() + " test.txt";
        system(command.c_str());

        return archive_name;
    }
};

TEST_F(ArchiveUtilsTest, ExtractTarGzFromFile) {
    std::string archive_path = create_test_tar_gz();
    std::string extract_path = (temp_dir / "extract").string();
    std::filesystem::create_directory(extract_path);

    ASSERT_TRUE(ArchiveUtils::extract_tar_gz_from_file(archive_path, extract_path));

    // Check if the extracted file exists and has the correct content
    std::string extracted_file = (std::filesystem::path(extract_path) / "test.txt").string();
    ASSERT_TRUE(std::filesystem::exists(extracted_file));

    std::ifstream file(extracted_file);
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "This is a test file content.");
}

TEST_F(ArchiveUtilsTest, ExtractTarGzFromMemory) {
    std::string archive_path = create_test_tar_gz();
    std::string extract_path = (temp_dir / "extract_memory").string();
    std::filesystem::create_directory(extract_path);

    // Read the archive content into memory
    std::ifstream archive_file(archive_path, std::ios::binary);
    std::string archive_content((std::istreambuf_iterator<char>(archive_file)), std::istreambuf_iterator<char>());

    ASSERT_TRUE(ArchiveUtils::extract_tar_gz_from_memory(archive_content, extract_path));

    // Check if the extracted file exists and has the correct content
    std::string extracted_file = (std::filesystem::path(extract_path) / "test.txt").string();
    ASSERT_TRUE(std::filesystem::exists(extracted_file));

    std::ifstream file(extracted_file);
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "This is a test file content.");
}

TEST_F(ArchiveUtilsTest, ExtractTarGzFromFileInvalidPath) {
    std::string invalid_path = (temp_dir / "nonexistent.tar.gz").string();
    std::string extract_path = (temp_dir / "extract_invalid").string();
    std::filesystem::create_directory(extract_path);

    ASSERT_FALSE(ArchiveUtils::extract_tar_gz_from_file(invalid_path, extract_path));
}

TEST_F(ArchiveUtilsTest, ExtractTarGzFromMemoryInvalidContent) {
    std::string invalid_content = "This is not a valid tar.gz content";
    std::string extract_path = (temp_dir / "extract_invalid_memory").string();
    std::filesystem::create_directory(extract_path);

    ASSERT_FALSE(ArchiveUtils::extract_tar_gz_from_memory(invalid_content, extract_path));
}

TEST_F(ArchiveUtilsTest, VerifyTarGzArchive) {
    std::string archive_path = create_test_tar_gz();
    std::ifstream archive_file(archive_path, std::ios::binary);
    std::string archive_content((std::istreambuf_iterator<char>(archive_file)), std::istreambuf_iterator<char>());
    ASSERT_TRUE(ArchiveUtils::verify_tar_gz_archive(archive_content));
}

TEST_F(ArchiveUtilsTest, VerifyTarGzArchiveInvalid) {
    std::string invalid_content = "This is not a valid tar.gz content";
    ASSERT_FALSE(ArchiveUtils::verify_tar_gz_archive(invalid_content));
}