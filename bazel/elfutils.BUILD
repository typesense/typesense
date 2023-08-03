load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

configure_make(
    name = "libdw",
    configure_options = [
        "--disable-libdebuginfod",
        "--disable-debuginfod",
        "--without-lzma",
        "--without-bzlib",
    ],
    lib_source = "//:all_srcs",
    out_lib_dir = "lib",
    out_static_libs = ["libdw.a", "libelf.a"],
    deps = [
        "@com_github_madler_zlib//:zlib",
    ],
)
