load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])


filegroup(
    name = "sentencepiece_src",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)



cmake(
    name = "sentencepiece",
    lib_source = "//:sentencepiece_src",
    out_static_libs = ["libsentencepiece.a"],
    out_include_dir = "include",
    build_args = [
        "--config Release",
        "--target sentencepiece-static",
    ],
    install = False,
    cache_entries = {
        'SPM_USE_BUILTIN_PROTOBUF': 'OFF',
    },
    postfix_script= """
echo "Intstalling sentencepiece"
cp $BUILD_TMPDIR/src/libsentencepiece.a $INSTALLDIR/lib/libsentencepiece.a
"""
)
