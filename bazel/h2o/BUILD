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
    name = "h2o",
    build_args = ["-j8"],
    cache_entries = select({
        "@platforms//os:linux": CMAKE_LINUX_CACHE_ENTRIES,
        "@platforms//os:macos": CMAKE_MACOS_CACHE_ENTRIES,
        "//conditions:default": CMAKE_CACHE_ENTRIES,
    }),
    generate_args = [
        "-DCMAKE_FIND_LIBRARY_SUFFIXES=.a",
        "-DCMAKE_CXX_FLAGS=-DNDEBUG",
        "-DCMAKE_C_FLAGS=-DNDEBUG",
        "-DWITH_MRUBY=OFF",
        "-DWITH_CCACHE=OFF",
    ],
    lib_source = "//:all_srcs",
    out_lib_dir = "../h2o.build_tmpdir",
    out_static_libs = ["libh2o-evloop.a"],
    targets = ["libh2o-evloop"],
    deps = [
        "@picotls_openssl",
        "@quicly",
    ],
)
