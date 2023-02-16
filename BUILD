load("@com_grail_bazel_compdb//:defs.bzl", "compilation_database")
load("@com_grail_bazel_output_base_util//:defs.bzl", "OUTPUT_BASE")

# Target to generate a compile_commands.json compilation database file
compilation_database(
    name = "compdb",
    output_base = OUTPUT_BASE,
    targets = [
        "//:typesense-server",
        "//:search",
        "//:benchmark",
    ],
)

filegroup(
    name = "src_files",
    srcs = glob(["src/*.cpp"]),
)

cc_library(
    name = "headers",
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    includes = ["include"],
)

cc_library(
    name = "common_deps",
    defines = [
        "NDEBUG",
    ],
    linkopts = select({
        "@platforms//os:macos": ["-framework Foundation -framework SystemConfiguration"],
        "//conditions:default": [],
    }),
    deps = [
        ":headers",
        "@com_github_brpc_braft//:braft",
        "@com_github_brpc_brpc//:brpc",
        "@com_github_google_glog//:glog",
        "@curl",
        "@for",
        "@h2o",
        "@iconv",
        "@icu",
        "@jemalloc",
        "@kakasi",
        "@lrucache",
        "@rocksdb",
        "@s2geometry",
        "@hnsw",
    ],
)

COPTS = [
    "-Wall",
    "-Wextra",
    "-Wno-unused-parameter",
    "-Werror=return-type",
    "-O2",
    "-g",
]

cc_binary(
    name = "typesense-server",
    srcs = [
        "src/main/typesense_server.cpp",
        ":src_files",
    ],
    local_defines = [
        "TYPESENSE_VERSION=\\\"$(TYPESENSE_VERSION)\\\""
    ],
    linkopts = select({
        "@platforms//os:linux": ["-static-libstdc++", "-static-libgcc"],
        "//conditions:default": [],
    }),
    copts = COPTS,
    deps = [":common_deps"],
)

cc_binary(
    name = "search",
    srcs = [
        "src/main/main.cpp",
        ":src_files",
    ],
    copts = COPTS,
    deps = [":common_deps"],
)

cc_binary(
    name = "benchmark",
    srcs = [
        "src/main/benchmark.cpp",
        ":src_files",
    ],
    copts = COPTS,
    deps = [":common_deps"],
)

filegroup(
    name = "test_src_files",
    srcs = glob(["test/*.cpp"]),
)

filegroup(
    name = "test_data_files",
    srcs = glob([
        "test/**/*.txt",
        "test/**/*.ini",
        "test/**/*.jsonl",
    ]),
)

cc_test(
    name = "typesense-test",
    srcs = [
        ":src_files",
        ":test_src_files",
    ],
    copts = COPTS,
    data = [
        ":test_data_files",
        "@libart//:data",
        "@token_offsets//file",
    ],
    deps = [
        ":common_deps",
        "@com_google_googletest//:gtest",
    ],
    defines = [
        "ROOT_DIR="
    ],
)
