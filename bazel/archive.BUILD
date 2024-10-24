load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cmake(
    name = "archive",
    cache_entries = {
        "CMAKE_BUILD_TYPE": "Release",
        "ENABLE_BZip2": "OFF",
        "ENABLE_LIBXML2": "OFF",
        "ENABLE_LZMA": "OFF",
        "ENABLE_OPENSSL": "OFF",
        "ENABLE_ZLIB": "OFF",
        "ENABLE_LZ4": "OFF",
        "ENABLE_ZSTD": "OFF",
    },
    lib_source = ":all_srcs",
    out_static_libs = ["libarchive.a"],
    visibility = ["//visibility:public"],
)