# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

genrule(
    name = "crc32c_config_h",
    srcs = ["src/crc32c_config.h.in"],
    outs = ["crc32c/crc32c_config.h"],
    cmd = """
sed -e 's/#cmakedefine01/#define/' \
""" + select({
        "@//bazel/config:brpc_with_sse42": """-e 's/ HAVE_SSE42/ HAVE_SSE42 1/' \
""",
        "//conditions:default": """-e 's/ HAVE_SSE42/ HAVE_SSE42 0/' \
""",
    }) + select({
        "@//bazel/config:brpc_with_glog": """-e 's/ CRC32C_TESTS_BUILT_WITH_GLOG/ CRC32C_TESTS_BUILT_WITH_GLOG 1/' \
""",
        "//conditions:default": """-e 's/ CRC32C_TESTS_BUILT_WITH_GLOG/ CRC32C_TESTS_BUILT_WITH_GLOG 0/' \
""",
    }) + """-e 's/ BYTE_ORDER_BIG_ENDIAN/ BYTE_ORDER_BIG_ENDIAN 0/' \
    -e 's/ HAVE_BUILTIN_PREFETCH/ HAVE_BUILTIN_PREFETCH 0/' \
    -e 's/ HAVE_MM_PREFETCH/ HAVE_MM_PREFETCH 0/' \
    -e 's/ HAVE_ARM64_CRC32C/ HAVE_ARM64_CRC32C 0/' \
    -e 's/ HAVE_STRONG_GETAUXVAL/ HAVE_STRONG_GETAUXVAL 0/' \
    -e 's/ HAVE_WEAK_GETAUXVAL/ HAVE_WEAK_GETAUXVAL 0/' \
    < $< > $@
""",
)

cc_library(
    name = "crc32c",
    srcs = [
        "src/crc32c.cc",
        "src/crc32c_arm64.cc",
        "src/crc32c_arm64.h",
        "src/crc32c_arm64_check.h",
        "src/crc32c_internal.h",
        "src/crc32c_portable.cc",
        "src/crc32c_prefetch.h",
        "src/crc32c_read_le.h",
        "src/crc32c_round_up.h",
        "src/crc32c_sse42.cc",
        "src/crc32c_sse42.h",
        "src/crc32c_sse42_check.h",
        ":crc32c_config_h",
    ],
    hdrs = [
        "include/crc32c/crc32c.h",
    ],
    copts = select({
        "@//bazel/config:brpc_with_sse42": ["-msse4.2"],
        "//conditions:default": [],
    }),
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)

cc_test(
    name = "crc32c_test",
    srcs = [
        "src/crc32c_arm64_unittest.cc",
        "src/crc32c_extend_unittests.h",
        "src/crc32c_portable_unittest.cc",
        "src/crc32c_prefetch_unittest.cc",
        "src/crc32c_read_le_unittest.cc",
        "src/crc32c_round_up_unittest.cc",
        "src/crc32c_sse42_unittest.cc",
        "src/crc32c_test_main.cc",
        "src/crc32c_unittest.cc",
    ],
    deps = [
        ":crc32c",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ] + select({
        "@//bazel/config:brpc_with_glog": ["@com_github_google_glog//:glog"],
        "//conditions:default": [],
    }),
)
