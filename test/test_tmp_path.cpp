#include "test_tmp_path.h"

#include <cstdlib>

std::string test_tmp_path(const std::string& sub) {
    const char* tmp = std::getenv("TEST_TMPDIR");
    return std::string(tmp != nullptr && tmp[0] != '\0' ? tmp : "/tmp") + "/typesense_test" + sub;
}

std::string test_models_dir() {
    const char* shard = std::getenv("TEST_SHARD_INDEX");
    return std::string("/tmp/typesense_test_models/shard_") + (shard != nullptr && shard[0] != '\0' ? shard : "0");
}
