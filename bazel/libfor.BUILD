package(default_visibility = ["//visibility:public"])

genrule(
    name = "gen_for-gen.c",
    srcs = [ "//:gen.pl" ],
    outs = [ "for-gen.c" ],
    cmd = "perl $< > $@",
)

cc_library(
    name = "for_gen",
    hdrs = [ ":gen_for-gen.c" ],
)

cc_library(
    name = "for",
    srcs = [ "for.c" ],
    hdrs = [ "for.h" ],
    include_prefix = "third_party/libfor",
    includes = [ "." ],
    deps = [ ":for_gen" ],
)
