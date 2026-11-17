#pragma once

#include <string>

// Directory for a test's on-disk state, `sub` appended. Under Bazel it is inside TEST_TMPDIR, which is
// private to each test shard, so shards running at the same time never share files; elsewhere /tmp.
std::string test_tmp_path(const std::string& sub = "");

// Directory for downloaded embedding models. It outlives the run, so a model downloads once, not on every run,
// and each test shard has its own: a model is written in place, so shards running at once can't share one.
std::string test_models_dir();
