load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

CMAKE_CACHE_ENTRIES = {
    "BUILD_SHARED_LIBS": "OFF",
    "CMAKE_BUILD_TYPE": "Release",
}

CMAKE_MACOS_CACHE_ENTRIES = dict(CMAKE_CACHE_ENTRIES.items() + {}.items())

CMAKE_LINUX_CACHE_ENTRIES = dict(CMAKE_CACHE_ENTRIES.items() + {
    "CMAKE_C_FLAGS": "-fPIC",
}.items())

cmake(
    name = "rocksdb",
    build_args = ["-j8"],
    cache_entries = select({
        "@platforms//os:linux": CMAKE_LINUX_CACHE_ENTRIES,
        "@platforms//os:macos": CMAKE_MACOS_CACHE_ENTRIES,
        "//conditions:default": CMAKE_CACHE_ENTRIES,
    }),
    generate_args = [
        "-DWITH_GFLAGS=OFF",
        "-DWITH_ALL_TESTS=OFF",
        "-DPORTABLE=1",
        "-DWITH_SNAPPY=1",
        "-DROCKSDB_BUILD_SHARED=OFF",
        "-DWITH_TESTS=OFF",
        "-DWITH_TOOLS=OFF",
        "-DUSE_RTTI=1",
    ] + select({
         "@platforms//os:macos": ["-DCMAKE_CXX_FLAGS=-Wno-error=uninitialized"],
         "//conditions:default": ["-DCMAKE_CXX_FLAGS=-Wno-error=maybe-uninitialized"],
    }),
    lib_source = "//:all_srcs",
    targets = ["rocksdb"],
    out_static_libs = ["librocksdb.a"],
    deps = ["@com_github_google_snappy//:snappy"],
)
