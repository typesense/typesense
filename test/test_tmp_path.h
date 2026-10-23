#pragma once

#include <string>

// Directory for a test's on-disk state, `sub` appended. Under Bazel it is inside TEST_TMPDIR, which is
// private to each test shard, so shards running at the same time never share files; elsewhere /tmp.
std::string test_tmp_path(const std::string& sub = "");
