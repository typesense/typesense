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

config_setting(
    name = "with_cuda",
    define_values = { "use_cuda": "on" }
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
        "@onnx_runtime//:onnxruntime_lib",
        "@sentencepiece",
        "@sentencepiece//:sentencepiece_headers",
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
        "@clip_tokenizer//:clip",
        "@whisper.cpp//:whisper",
        "@whisper.cpp//:whisper_headers",
        "@snowball",
        "@snowball//:snowball_headers",
        "@archive",
        # "@zip",
    ])

cc_library(
    name = "linux_deps",
    defines = [
        "NDEBUG",
    ],
    deps = [
        "@elfutils//:libdw",
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

ASAN_COPTS = [
    "-fsanitize=address",
    "-fno-omit-frame-pointer",
    "-DASAN_BUILD"
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
        "@platforms//os:linux": ["-static-libstdc++", "-static-libgcc", "-fuse-ld=lld"],
        "@platforms//os:macos": ["-framework Foundation", "-framework Accelerate", "-framework Metal", "-framework MetalKit"],
        "//conditions:default": [],
    }),
    copts = COPTS + select({
        "@platforms//os:linux": ["-DBACKWARD_HAS_DW=1", "-DBACKWARD_HAS_UNWIND=1"],
        "//conditions:default": [],
    }),
    deps = [":common_deps"] +  select({
        "@platforms//os:linux": [":linux_deps"],
        "//conditions:default": [],
    }),
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
        "test/**/*.gz",
    ]),
)

TEST_COPTS = [
    "-Wall",
    "-Wextra",
    "-Wno-unused-parameter",
    "-Werror=return-type",
    "-g",
    "-DTEST_BUILD"
]

ASAN_COPTS = [
    "-fsanitize=address",
    "-fno-omit-frame-pointer",
    "-DASAN_BUILD"
]

config_setting(
    name = "release_mode",
    define_values = { "mode": "release" }
)

config_setting(
    name = "asan_mode",
    define_values = { "mode": "asan" }
)

cc_test(
    name = "typesense-test",
    srcs = [
        ":src_files",
        ":test_src_files",
    ],
    copts = TEST_COPTS + select({
        ":release_mode": ["-O2"],
        ":asan_mode": ["-O0"] + ASAN_COPTS,
        "//conditions:default": ["-O0"]
    }),
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
    linkopts = select({
       ":asan_mode": ["-fsanitize=address", "-fuse-ld=lld"],
       "//conditions:default": []
    }) +  select({
       "@platforms//os:linux": ["-fuse-ld=lld"],
       "//conditions:default": [],
   })
)
