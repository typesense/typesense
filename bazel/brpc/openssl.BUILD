# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copyright 2016 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copied from https://github.com/bazelbuild/rules_foreign_cc/blob/0.7.0/examples/third_party/openssl/BUILD.openssl.bazel
#
# Modifications:
# 1. Create alias `ssl` & `crypto` to align with boringssl.
# 2. Build with `@com_github_madler_zlib//:zlib`.
# 3. Add more configure options coming from debian openssl package configurations.

load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make", "configure_make_variant")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
)

CONFIGURE_OPTIONS = [
    "no-idea",
    "no-mdc2",
    "no-rc5",
    "no-ssl3",
    "no-ssl3-method",
    "enable-rfc3779",
    "enable-cms",
    "no-capieng",
    "enable-ec_nistp_64_gcc_128",
    "--with-zlib-include=$$EXT_BUILD_DEPS$$",
    "--with-zlib-lib=$$EXT_BUILD_DEPS$$",
    # https://stackoverflow.com/questions/36220341/struct-in6-addr-has-no-member-named-s6-addr32-with-ansi
    "-D_DEFAULT_SOURCE=1",
    "-DPEDANTIC",
]

LIB_NAME = "openssl"

MAKE_TARGETS = [
    "build_libs",
    "install_dev",
]

config_setting(
    name = "msvc_compiler",
    flag_values = {
        "@bazel_tools//tools/cpp:compiler": "msvc-cl",
    },
    visibility = ["//visibility:public"],
)

alias(
    name = "ssl",
    actual = "openssl",
    visibility = ["//visibility:public"],
)

alias(
    name = "crypto",
    actual = "openssl",
    visibility = ["//visibility:public"],
)

alias(
    name = "openssl",
    actual = select({
        ":msvc_compiler": "openssl_msvc",
        "//conditions:default": "openssl_default",
    }),
    visibility = ["//visibility:public"],
)

configure_make_variant(
    name = "openssl_msvc",
    build_data = [
        "@nasm//:nasm",
        "@perl//:perl",
    ],
    configure_command = "Configure",
    configure_in_place = True,
    configure_options = CONFIGURE_OPTIONS + [
        "VC-WIN64A",
        # Unset Microsoft Assembler (MASM) flags set by built-in MSVC toolchain,
        # as NASM is unsed to build OpenSSL rather than MASM
        "ASFLAGS=\" \"",
    ],
    configure_prefix = "$PERL",
    env = {
        # The Zi flag must be set otherwise OpenSSL fails to build due to missing .pdb files
        "CFLAGS": "-Zi",
        "PATH": "$$(dirname $(execpath @nasm//:nasm)):$$PATH",
        "PERL": "$(execpath @perl//:perl)",
    },
    lib_name = LIB_NAME,
    lib_source = ":all_srcs",
    out_static_libs = [
        "libssl.lib",
        "libcrypto.lib",
    ],
    targets = MAKE_TARGETS,
    toolchain = "@rules_foreign_cc//toolchains:preinstalled_nmake_toolchain",
    deps = [
        "@com_github_madler_zlib//:zlib",
    ],
)

# https://wiki.openssl.org/index.php/Compilation_and_Installation
configure_make(
    name = "openssl_default",
    configure_command = "config",
    configure_in_place = True,
    configure_options = CONFIGURE_OPTIONS,
    env = select({
        "@platforms//os:macos": {
            "AR": "",
            "PERL": "$$EXT_BUILD_ROOT$$/$(PERL)",
        },
        "//conditions:default": {
            "PERL": "$$EXT_BUILD_ROOT$$/$(PERL)",
        },
    }),
    lib_name = LIB_NAME,
    lib_source = ":all_srcs",
    # Note that for Linux builds, libssl must come before libcrypto on the linker command-line.
    # As such, libssl must be listed before libcrypto
    out_static_libs = [
        "libssl.a",
        "libcrypto.a",
    ],
    targets = MAKE_TARGETS,
    toolchains = ["@rules_perl//:current_toolchain"],
    deps = [
        "@com_github_madler_zlib//:zlib",
    ],
)

filegroup(
    name = "gen_dir",
    srcs = [":openssl"],
    output_group = "gen_dir",
)
