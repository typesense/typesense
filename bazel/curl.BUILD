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
    "OPENSSL_ROOT_DIR": "$$EXT_BUILD_DEPS$$/openssl",
    "CURL_USE_OPENSSL": "ON",
    "BUILD_CURL_EXE": "OFF",
    "CURL_DISABLE_LDAP": "ON",
    "CURL_DISABLE_LDAPS": "ON",
    "CURL_USE_LIBSSH2": "OFF",
    "CURL_USE_LIBPSL": "OFF",
    "USE_LIBIDN2": "OFF"
}

CMAKE_MACOS_CACHE_ENTRIES = dict(CMAKE_CACHE_ENTRIES.items() + {}.items())

CMAKE_LINUX_CACHE_ENTRIES = dict(CMAKE_CACHE_ENTRIES.items() + {
    "CMAKE_C_FLAGS": "-fPIC",
}.items())

cmake(
    name = "curl",
    build_args = ["-j8"],
    cache_entries = select({
        "@platforms//os:linux": CMAKE_LINUX_CACHE_ENTRIES,
        "@platforms//os:macos": CMAKE_MACOS_CACHE_ENTRIES,
        "//conditions:default": CMAKE_CACHE_ENTRIES,
    }),
    lib_source = "//:all_srcs",
    out_static_libs = ["libcurl.a"],
    deps = ["@openssl"],
)
