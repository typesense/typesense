cc_library(
    name = "headers",
    hdrs = glob(["includes/**"]),
    strip_include_prefix = "includes",
    visibility = ["//visibility:public"],
    deps = [
        "@github_nlohmann_json//:json",
    ],
)

cc_library(
    name = "operators",
    hdrs = glob(["operators/base64.h",
                 "operators/string_utils*",
                 "operators/ustring.h",
                 "operators/tokenizer/bert_tokenizer.hpp", 
                 "operators/tokenizer/basic_tokenizer.hpp"]),
    srcs = glob(["operators/base64.cc",
                 "operators/string_utils*",
                 "operators/ustring.cc",
                 "operators/tokenizer/bert_tokenizer.cc", 
                 "operators/tokenizer/basic_tokenizer.cc"]),
    strip_include_prefix = "operators",
    visibility = ["//visibility:public"],
)
