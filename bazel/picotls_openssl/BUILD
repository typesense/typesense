package(default_visibility = ["//visibility:public"])

cc_library(
    name = "picotls_openssl",
    srcs = [
        "lib/openssl.c",
        "lib/pembase64.c",
        "lib/picotls.c",
    ],
    hdrs = glob(["include/**/*.h"]),
    copts = [
        "-std=c99",
    ],
    includes = ["include"],
    deps = [
        "@openssl",
    ],
)
