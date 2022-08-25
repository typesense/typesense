load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

configure_make(
    name = "openssl",
    configure_command = "config",
    configure_in_place = True,
    configure_options = [
        "enable-rfc3779",
        "enable-cms",
        "enable-ec_nistp_64_gcc_128",
        "--libdir=lib",
        "no-shared",
        "--with-zlib-include=$$EXT_BUILD_DEPS$$",
        "--with-zlib-lib=$$EXT_BUILD_DEPS$$",
        # https://stackoverflow.com/questions/36220341/struct-in6-addr-has-no-member-named-s6-addr32-with-ansi
        "-D_DEFAULT_SOURCE=1",
        "-DPEDANTIC",
    ],
    env = select({
        "@platforms//os:macos": {
            "AR": "",
            "PERL": "$$EXT_BUILD_ROOT$$/$(PERL)",
        },
        "//conditions:default": {
            "PERL": "$$EXT_BUILD_ROOT$$/$(PERL)",
        },
    }),
    lib_name = "openssl",
    lib_source = ":all_srcs",
    out_lib_dir = "../openssl.build_tmpdir/openssl/lib",
    out_static_libs = [
        "libssl.a",
        "libcrypto.a",
    ],
    targets = [
        "build_libs",
        "install_dev",
    ],
    toolchains = ["@rules_perl//:current_toolchain"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_madler_zlib//:zlib",
    ],
)

alias(
    name = "crypto",
    actual = "openssl",
    visibility = ["//visibility:public"],
)

alias(
    name = "ssl",
    actual = "openssl",
    visibility = ["//visibility:public"],
)
