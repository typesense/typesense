#include "test_tmp_path.h"

#include <cstdlib>

std::string test_tmp_path(const std::string& sub) {
    const char* tmp = std::getenv("TEST_TMPDIR");
    return std::string(tmp != nullptr && tmp[0] != '\0' ? tmp : "/tmp") + "/typesense_test" + sub;
}
