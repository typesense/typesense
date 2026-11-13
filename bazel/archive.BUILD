load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")
load("@//bazel:homebrew.bzl", "CMAKE_MACOS_IGNORE_HOMEBREW")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

CMAKE_CACHE_ENTRIES = {
    "CMAKE_BUILD_TYPE": "Release",
    "ENABLE_BZip2": "OFF",
    "ENABLE_LIBXML2": "OFF",
    "ENABLE_LZMA": "OFF",
    "ENABLE_OPENSSL": "OFF",
    "ENABLE_ZLIB": "OFF",
    "ENABLE_LZ4": "OFF",
    "ENABLE_ZSTD": "OFF",
    "ENABLE_ICONV": "OFF",
}

cmake(
    name = "archive",
    cache_entries = select({
        "@platforms//os:macos": dict(CMAKE_CACHE_ENTRIES.items() + CMAKE_MACOS_IGNORE_HOMEBREW.items()),
        "//conditions:default": CMAKE_CACHE_ENTRIES,
    }),
    lib_source = ":all_srcs",
    out_static_libs = ["libarchive.a"],
    visibility = ["//visibility:public"],
)