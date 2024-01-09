load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "whisper_srcs",
    srcs = glob(["**"]),
)

config_setting(
    name = "with_cuda",
    define_values = { "use_cuda": "on" }
)

print("CUDA: " + str(select({
    ":with_cuda": True,
    "//conditions:default": False,
})))

load("@cuda_home_repo//:cuda_home.bzl", "CUDA_HOME")

cmake(
    name = "whisper",
    cache_entries= select({
        ":with_cuda": {
            'BUILD_SHARED_LIBS': 'OFF',
            'WHISPER_BUILD_EXAMPLES': 'OFF',
            'WHISPER_BUILD_TESTS' : 'OFF',
            'WHISPER_CUBLAS': 'ON',
            'CMAKE_CUDA_COMPILER': CUDA_HOME + "/bin/nvcc",
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
    out_static_libs = ["static/libwhisper.a"],
    tags=["requires-network","no-sandbox"],
)

cc_library(
    name = "whisper_headers",
    hdrs = glob(["*.h"]),
    visibility = ["//visibility:public"]
)
