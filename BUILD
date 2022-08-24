load("@rules_cc//cc:defs.bzl", "cc_binary")

cc_library(
    name = "ts_lib",
    srcs = glob([":/src/*.cpp"]),
    hdrs = glob([":include/*.h"]),
)

cc_binary(
    name = "typesense",
    srcs = glob([":/src/main/*.cpp"]),
    deps = [ "//:ts_lib" ],
)