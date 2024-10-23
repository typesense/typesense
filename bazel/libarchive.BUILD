load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:private"],
)

cmake(
    name = "libarchive",
    cache_entries = {
        "CMAKE_BUILD_TYPE": "Release",
        "ENABLE_BZip2": "ON",
        "ENABLE_LIBXML2": "ON",
        "ENABLE_LZMA": "ON",
        "ENABLE_OPENSSL": "ON",
        "ENABLE_ZLIB": "ON",
    },
    lib_source = ":all_srcs",
    out_static_libs = ["libarchive.a"],
    visibility = ["//visibility:public"],
)