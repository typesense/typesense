load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "kakasi_data",
    srcs = ["data/japanese_data.cpp"],
    hdrs = ["data/japanese_data.h"],
    includes = ["data"],
    visibility = ["//visibility:public"],
)

configure_make(
    name = "kakasi",
    configure_in_place = True,
    configure_options = ["--enable-shared=no"],
    env = select({
        # https://github.com/bazelbuild/rules_foreign_cc/issues/947#issuecomment-1208960469
        "@platforms//os:macos": {
            "AR": "",
        },
        "//conditions:default": {},
    }),
    lib_source = "@kakasi//:all_srcs",
    out_static_libs = ["libkakasi.a"],
    visibility = ["//visibility:public"],
    deps = [
        "@iconv",
        "@kakasi//:kakasi_data",
    ],
)
