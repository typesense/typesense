load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

configure_make(
    name = "jemalloc",
    args = ["-j8"],
    env = select({
        # https://github.com/bazelbuild/rules_foreign_cc/issues/947#issuecomment-1208960469
        "@platforms//os:macos": {
            "AR": "",
        },
        "//conditions:default": {},
    }),
    lib_source = ":all_srcs",
    out_include_dir = "include/jemalloc",
    out_static_libs = ["libjemalloc.a"],
)
