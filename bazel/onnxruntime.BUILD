filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "hdrs",
    hdrs = glob(["include/onnxruntime/**/*.h"]),
    strip_include_prefix = "include/onnxruntime",
    visibility = ["//visibility:public"],
)