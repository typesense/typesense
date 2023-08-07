load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")

load("//bazel:onnxruntime_cuda_defs.bzl", "cuda_home_repository")

cuda_home_repository(name = "cuda_home_repo")

git_repository(
    name = "com_grail_bazel_compdb",
    commit = "58672f5eecd70a2d3ed50016a3abf907701404e0",
    remote = "https://github.com/grailbio/bazel-compilation-database.git",
)

load("@com_grail_bazel_compdb//:deps.bzl", "bazel_compdb_deps")

bazel_compdb_deps()

http_archive(
    name = "rules_foreign_cc",
    patches = ["//bazel:foreign_cc.patch"],
    sha256 = "2a4d07cd64b0719b39a7c12218a3e507672b82a97b98c6a89d38565894cf7c51",
    strip_prefix = "rules_foreign_cc-0.9.0",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/refs/tags/0.9.0.tar.gz",
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

# This sets up some common toolchains for building targets. For more details, please see
# https://bazelbuild.github.io/rules_foreign_cc/0.9.0/flatten.html#rules_foreign_cc_dependencies
rules_foreign_cc_dependencies(
    cmake_version="3.25.0",
    ninja_version="1.11.1")

# brpc and its dependencies
git_repository(
    name = "com_github_brpc_brpc",
    commit = "70d702f1c7c4f663d30cd0ca284bf838a8cf7afb",
    patches = [
        "//bazel/brpc:brpc.patch",
    ],
    remote = "https://github.com/apache/brpc.git",
)


new_git_repository(
    name="onnx_runtime",
    branch= "rel-1.14.1",
    build_file = "//bazel:onnxruntime.BUILD",
    remote= "https://github.com/microsoft/onnxruntime",
    patches=["//bazel:onnx.patch"],
    patch_cmds= ["git submodule sync && git submodule foreach  'git fetch --tags' && git submodule update --init --remote"]
)

new_git_repository(
    name = "onnx_runtime_extensions",
    build_file = "//bazel:onnxruntime_extensions.BUILD",
    remote = "https://github.com/microsoft/onnxruntime-extensions",
    commit = "81e7799c69044c745239202085eb0a98f102937b",
    patches=["//bazel:onnx_ext.patch"],
)

new_git_repository(
    name = "com_github_madler_zlib",
    build_file = "//bazel:zlib.BUILD",
    remote = "https://github.com/madler/zlib.git",
    tag = "v1.2.12",
)

# new_git_repository(
#     name = "zip",
#     build_file = "//bazel:zip.BUILD",
#     branch = "master",
#     remote = "https://github.com/kuba--/zip.git",
# )

new_git_repository(
    name = "sentencepiece",
    build_file = "//bazel:sentencepiece.BUILD",
    tag = "v0.1.98",
    remote = "https://github.com/google/sentencepiece",
    patches = ["//bazel:sentencepiece.patch"],
    patch_args = ["-p1"]
)

git_repository(
    name = "rules_perl",
    remote = "https://github.com/bazelbuild/rules_perl.git",
    commit = "7f10dada09fcba1dc79a6a91da2facc25e72bd7d",
)

load("@rules_perl//perl:deps.bzl", "perl_register_toolchains", "perl_rules_dependencies")

perl_rules_dependencies()
perl_register_toolchains()

git_repository(
    name = "com_github_brpc_braft",
    commit = "bc527db96420f610257573d80e5f60a8b0d835ef",
    patches = ["//bazel/braft:0001.patch"],
    remote = "https://github.com/baidu/braft.git",
    repo_mapping = {
        "@zlib": "@com_github_madler_zlib",
    },
)

# Below are dependencies of brpc/braft and protobuf
http_archive(
    name = "rules_pkg",
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

git_repository(
    name = "com_google_protobuf",
    remote = "https://github.com/protocolbuffers/protobuf.git",
    repo_mapping = {
        "@zlib": "@com_github_madler_zlib",
    },
    tag = "v21.5",
)

http_archive(
    name = "com_github_google_leveldb",  # 2021-02-23T21:51:12Z
    build_file = "//bazel/leveldb:leveldb.BUILD",
    sha256 = "9a37f8a6174f09bd622bc723b55881dc541cd50747cbd08831c2a82d620f6d76",
    strip_prefix = "leveldb-1.23",
    urls = [
        "https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz",
    ],
)

http_archive(
    name = "com_github_google_crc32c",  # 2021-10-05T19:47:30Z
    build_file = "//bazel:crc32c.BUILD",
    sha256 = "ac07840513072b7fcebda6e821068aa04889018f24e10e46181068fb214d7e56",
    strip_prefix = "crc32c-1.1.2",
    urls = ["https://github.com/google/crc32c/archive/1.1.2.tar.gz"],
)

http_archive(
    name = "com_github_google_snappy",  # 2017-08-25
    build_file = "//bazel:snappy.BUILD",
    sha256 = "3dfa02e873ff51a11ee02b9ca391807f0c8ea0529a4924afa645fbf97163f9d4",
    strip_prefix = "snappy-1.1.7",
    urls = [
        "https://storage.googleapis.com/mirror.tensorflow.org/github.com/google/snappy/archive/1.1.7.tar.gz",
        "https://github.com/google/snappy/archive/1.1.7.tar.gz",
    ],
)

new_git_repository(
    name = "for",
    build_file = "//bazel:libfor.BUILD",
    commit = "49611808d08d4e47116aa2a3ddcabeb418f405f7",
    remote = "https://github.com/cruppstahl/libfor.git",
)

new_git_repository(
    name = "lrucache",
    build_file = "//bazel:lrucache.BUILD",
    commit = "13f30ad33a227a3e9682578c450777380ecddfcf",
    remote = "https://github.com/goldsborough/lru-cache.git",
)

new_git_repository(
    name = "kakasi",
    build_file = "//bazel:kakasi.BUILD",
    commit = "77f2d1ce0146d15199ae0db1e61e0b699b0b55f6",
    remote = "https://github.com/typesense/kakasi.git",
)

new_git_repository(
    name = "hnsw",
    build_file = "//bazel:hnsw.BUILD",
    commit = "5aba40d4b10dd77aece2ab9a1b3fdf06e433466a",
    remote = "https://github.com/typesense/hnswlib.git",
)

http_archive(
    name = "com_github_gflags_gflags",
    sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
    strip_prefix = "gflags-2.2.2",
    urls = ["https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"],
)

http_archive(
    name = "com_github_google_glog",
    sha256 = "122fb6b712808ef43fbf80f75c52a21c9760683dae470154f02bddfc61135022",
    strip_prefix = "glog-0.6.0",
    urls = ["https://github.com/google/glog/archive/v0.6.0.zip"],
)

new_git_repository(
    name = "rocksdb",
    build_file = "//bazel:rocksdb.BUILD",
    remote = "https://github.com/facebook/rocksdb.git",
    tag = "v7.8.3",
)

http_archive(
    name = "curl",
    build_file = "//bazel:curl.BUILD",
    sha256 = "6147ac0b22f8c11cbd3933d7fec064dee373402c3705193ceb703a5a665f2e0c",
    strip_prefix = "curl-7.87.0",
    urls = ["https://github.com/curl/curl/releases/download/curl-7_87_0/curl-7.87.0.zip"],
)

new_git_repository(
    name = "h2o",
    build_file = "//bazel/h2o:BUILD",
    commit = "1491a703195790278091fd7aee547fbba78e89af",
    patches = ["//bazel/h2o:h2o_1491a703195790278091fd7aee547fbba78e89af.patch"],
    remote = "https://github.com/h2o/h2o.git",
)

http_archive(
    name = "openssl",
    build_file = "//bazel:openssl3.BUILD",
    sha256 = "aa7d8d9bef71ad6525c55ba11e5f4397889ce49c2c9349dcea6d3e4f0b024a7a",
    strip_prefix = "openssl-3.0.5",
    urls = ["https://www.openssl.org/source/openssl-3.0.5.tar.gz"],
)

http_archive(
    name = "jemalloc",
    build_file = "//bazel:jemalloc.BUILD",
    sha256 = "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa",
    strip_prefix = "jemalloc-5.3.0",
    urls = ["https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2"],
)

new_git_repository(
    name = "s2geometry",
    build_file = "//bazel/s2geometry:BUILD",
    commit = "efb124d8eaf3433323d3e877dedd5e94a63339a3",
    patches = ["//bazel/s2geometry:0001.patch"],
    remote = "https://github.com/google/s2geometry.git",
)

new_git_repository(
    name = "icu",
    build_file = "//bazel/icu:BUILD",
    patches = ["//bazel/icu:icu.patch"],
    remote = "https://github.com/unicode-org/icu.git",
    tag = "release-71-1",
)

git_repository(
    name = "com_google_googletest",
    remote = "https://github.com/google/googletest.git",
    tag = "release-1.12.1",
)

new_git_repository(
    name = "libart",
    build_file = "//bazel:libart.BUILD",
    commit = "bbbf588bca55bce095538ee8ca8b422904baebc5",
    remote = "https://github.com/kishorenc/libart.git",
)

new_git_repository(
    name = "picotls_openssl",
    build_file = "//bazel/picotls_openssl:BUILD",
    commit = "7970614ad049d194fe1691bdf0cc66c6930a3a2f",  # 07/21/2022
    patches = ["//bazel/picotls_openssl:0001.patch"],
    remote = "https://github.com/h2o/picotls.git",
)

new_git_repository(
    name = "quicly",
    build_file = "//bazel/quicly:BUILD",
    commit = "46110287eb20e0780cf41bd30fc4715907ccf400",  # 08/08/2022
    patches = ["//bazel/quicly:0001.patch"],
    remote = "https://github.com/h2o/quicly.git",
)

new_git_repository(
    name = "klib",
    build_file = "//bazel/klib:BUILD",
    commit = "de09fb7dff67be7c1a58e5be9fee4b4a9ca3a265",  # 03/04/2017
    remote = "https://github.com/attractivechaos/klib.git",
)

http_archive(
    name = "iconv",
    build_file = "//bazel:iconv.BUILD",
    sha256 = "8f74213b56238c85a50a5329f77e06198771e70dd9a739779f4c02f65d971313",
    strip_prefix = "libiconv-1.17",
    urls = ["https://ftp.gnu.org/pub/gnu/libiconv/libiconv-1.17.tar.gz"],
)

http_file(
  name = "token_offsets",
  downloaded_file_path = "token_offsets.txt",
  sha256 = "55c1c510ca6335c049f5696f3b94ac7be61e84f3e27cd8169021929b3db99651",
  urls = ["https://gist.githubusercontent.com/kishorenc/1d330714eb07019f210f16ccb3991217/raw/bd52e05375d305d5aaa7ac06219af999726933a4/token_offsets.log"],
)

http_archive(
    name = "elfutils",
    build_file = "//bazel:elfutils.BUILD",
    sha256 = "ecc406914edf335f0b7fc084ebe6c460c4d6d5175bfdd6688c1c78d9146b8858",
    strip_prefix = "elfutils-0.182",
    urls = ["https://sourceware.org/elfutils/ftp/0.182/elfutils-0.182.tar.bz2"],
)