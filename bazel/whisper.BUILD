load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "whisper_srcs",
    srcs = glob(["**"]),
)


cmake(
    name = "whisper",
    cache_entries= select({
        ":wih_cuda": {
            'BUILD_SHARED_LIBS': 'OFF',
            'WHISPER_BUILD_EXAMPLES': 'OFF',
            'WHISPER_BUILD_TESTS': 'OFF'
            'WHISPER_BUILD_CUBLAS': 'ON'
        },
        "//conditions:default": {
            'BUILD_SHARED_LIBS': 'OFF',
            'WHISPER_BUILD_EXAMPLES': 'OFF',
            'WHISPER_BUILD_TESTS': 'OFF'
        },
    }), 
    build_args = [
        "--", "-j8"
    ],
    lib_source = "//:whisper_srcs",
    tags = ["no-sandbox"],
    out_static_libs = ["static/libwhisper.a"],
)

cc_library(
    name = "whisper_headers",
    hdrs = glob(["*.h"]),
    visibility = ["//visibility:public"]
)
