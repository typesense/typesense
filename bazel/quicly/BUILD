package(default_visibility = ["//visibility:public"])

cc_library(
    name = "quicly",
    srcs = [
        "lib/cc-cubic.c",
        "lib/cc-pico.c",
        "lib/cc-reno.c",
        "lib/defaults.c",
        "lib/frame.c",
        "lib/local_cid.c",
        "lib/loss.c",
        "lib/quicly.c",
        "lib/ranges.c",
        "lib/rate.c",
        "lib/recvstate.c",
        "lib/remote_cid.c",
        "lib/retire_cid.c",
        "lib/sendstate.c",
        "lib/sentmap.c",
        "lib/streambuf.c",
    ],
    hdrs = glob(["include/**/*.h"]),
    copts = [
        "-std=c99",
    ],
    includes = ["include"],
    linkopts = ["-lm"],
    deps = [
        "@klib",
        "@picotls_openssl",
    ],
)
