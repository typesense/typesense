load("@rules_foreign_cc//foreign_cc:defs.bzl", "make")

package(default_visibility = ["//visibility:public"])


filegroup(
    name = "snowball_src",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "snowball_headers",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    visibility = ["//visibility:public"],
    strip_include_prefix = "include"
)


make(
    name = "snowball",
    lib_source = "//:snowball_src",
    out_static_libs = ["libstemmer.a"],
    out_include_dir = "include",
    env = select({
        "@platforms//os:macos": {
            "AR": "/usr/bin/ar",
        },
        "//conditions:default": {},
    }),
    args= ["-j12"],
    tags = ["no-sandbox"],
    targets = [""],
    postfix_script= """
        echo "Installing snowball"
        cp $BUILD_TMPDIR/libstemmer.a $INSTALLDIR/lib/libstemmer.a
    """
)
