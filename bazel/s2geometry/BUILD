load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

CMAKE_CACHE_ENTRIES = {
    "BUILD_SHARED_LIBS": "OFF",
    "CMAKE_BUILD_TYPE": "Release",
    "CMAKE_USE_OPENSSL": "ON",
    "OPENSSL_ROOT_DIR": "$$EXT_BUILD_DEPS$$/openssl",
}

CMAKE_MACOS_CACHE_ENTRIES = CMAKE_CACHE_ENTRIES

CMAKE_LINUX_CACHE_ENTRIES = dict(CMAKE_CACHE_ENTRIES.items() + {
    "CMAKE_C_FLAGS": "-fPIC",
}.items())

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cmake(
    name = "s2geometry",
    build_args = ["-j8"],
    cache_entries = select({
        "@platforms//os:linux": CMAKE_LINUX_CACHE_ENTRIES,
        "@platforms//os:macos": CMAKE_MACOS_CACHE_ENTRIES,
        "//conditions:default": CMAKE_CACHE_ENTRIES,
    }),
    defines = ["S2_USE_GLOG"],
    generate_args = [
        "-DCMAKE_FIND_LIBRARY_SUFFIXES=.a",
        "-DBUILD_SHARED_LIBS=OFF",
    ],
    lib_source = ":all_srcs",
    out_static_libs = ["libs2.a"],
    targets = ["s2"],
    deps = [
        "@openssl",
    ],
)
