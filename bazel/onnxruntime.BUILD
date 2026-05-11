filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "hdrs",
    hdrs = glob(["include/onnxruntime/**/*.h"]),
    strip_include_prefix = "include/onnxruntime",
    visibility = ["//visibility:public"],
)

load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

__POSTFIX = """
mkdir -p $INSTALLDIR/lib
mkdir -p $INSTALLDIR/lib/_deps
mkdir -p $INSTALLDIR/lib/_deps/onnx-build
mkdir -p $INSTALLDIR/lib/_deps/re2-build
mkdir -p $INSTALLDIR/lib/_deps/abseil_cpp-build
mkdir -p $INSTALLDIR/lib/_deps/protobuf-build
mkdir -p $INSTALLDIR/lib/_deps/abseil_cpp-build/absl/container
mkdir -p $INSTALLDIR/lib/_deps/abseil_cpp-build/absl/hash
mkdir -p $INSTALLDIR/lib/_deps/pytorch_cpuinfo-build
mkdir -p $INSTALLDIR/lib/_deps/pytorch_cpuinfo-build/deps
mkdir -p $INSTALLDIR/lib/_deps/pytorch_cpuinfo-build/deps/clog
mkdir -p $INSTALLDIR/lib/_deps/google_nsync-build
mkdir -p $INSTALLDIR/lib/_deps/opencv-build
mkdir -p $INSTALLDIR/lib/_deps/opencv-build/lib
mkdir -p $INSTALLDIR/lib/_deps/opencv-build/3rdparty
mkdir -p $INSTALLDIR/lib/_deps/opencv-build/3rdparty/lib
cp $BUILD_TMPDIR/_deps/onnx-build/libonnx.a $INSTALLDIR/lib/_deps/onnx-build
cp $BUILD_TMPDIR/_deps/onnx-build/libonnx_proto.a $INSTALLDIR/lib/_deps/onnx-build
cp $BUILD_TMPDIR/_deps/re2-build/libre2.a $INSTALLDIR/lib/_deps/re2-build
cp -r $BUILD_TMPDIR/_deps/abseil_cpp-build/. $INSTALLDIR/lib/_deps/abseil_cpp-build
cp $BUILD_TMPDIR/_deps/google_nsync-build/libnsync_cpp.a $INSTALLDIR/lib/_deps/google_nsync-build
cp $BUILD_TMPDIR/_deps/pytorch_cpuinfo-build/deps/clog/libclog.a $INSTALLDIR/lib/_deps/pytorch_cpuinfo-build/deps/clog
cp $BUILD_TMPDIR/_deps/pytorch_cpuinfo-build/libcpuinfo.a $INSTALLDIR/lib/_deps/pytorch_cpuinfo-build
cp $BUILD_TMPDIR/_deps/opencv-build/lib/libopencv_imgcodecs.a $INSTALLDIR/lib/_deps/opencv-build/lib
cp $BUILD_TMPDIR/_deps/opencv-build/lib/libopencv_imgproc.a $INSTALLDIR/lib/_deps/opencv-build/lib
cp $BUILD_TMPDIR/_deps/opencv-build/lib/libopencv_core.a $INSTALLDIR/lib/_deps/opencv-build/lib
cp $BUILD_TMPDIR/_deps/opencv-build/3rdparty/lib/liblibjpeg-turbo.a $INSTALLDIR/lib/_deps/opencv-build/3rdparty/lib
cp $BUILD_TMPDIR/_deps/opencv-build/3rdparty/lib/liblibpng.a $INSTALLDIR/lib/_deps/opencv-build/3rdparty/lib
cp $BUILD_TMPDIR/lib/libnoexcep_operators.a $INSTALLDIR/lib
cp $BUILD_TMPDIR/lib/libocos_operators.a $INSTALLDIR/lib
cp $BUILD_TMPDIR/lib/libortcustomops.a $INSTALLDIR/lib
"""

__POSTFIX_WITH_CUDA = __POSTFIX + """
cp $BUILD_TMPDIR/libonnxruntime_providers_shared.so $INSTALLDIR/../../../
cp $BUILD_TMPDIR/libonnxruntime_providers_cuda.so $INSTALLDIR/../../../
"""


load("@cuda_home_repo//:cuda_home.bzl", "CUDA_HOME")
load("@cuda_home_repo//:cudnn_home.bzl", "CUDNN_HOME")

__ONNXRUNTIME_WITHOUT_CUDA = {'onnxruntime_RUN_ONNX_TESTS':'OFF',
'onnxruntime_GENERATE_TEST_REPORTS':'ON',
'onnxruntime_USE_MIMALLOC':'OFF',
'onnxruntime_ENABLE_PYTHON':'OFF',
'onnxruntime_BUILD_CSHARP':'OFF',
'onnxruntime_BUILD_JAVA':'OFF',
'onnxruntime_BUILD_NODEJS':'OFF',
'onnxruntime_BUILD_OBJC':'OFF',
'onnxruntime_BUILD_SHARED_LIB':'OFF',
'onnxruntime_BUILD_APPLE_FRAMEWORK':'OFF',
'onnxruntime_USE_DNNL':'OFF',
'onnxruntime_USE_NNAPI_BUILTIN':'OFF',
'onnxruntime_USE_RKNPU':'OFF',
'onnxruntime_USE_LLVM':'OFF',
'onnxruntime_ENABLE_MICROSOFT_INTERNAL':'OFF',
'onnxruntime_USE_VITISAI':'OFF',
'onnxruntime_USE_TENSORRT':'OFF',
'onnxruntime_SKIP_AND_PERFORM_FILTERED_TENSORRT_TESTS':'ON',
'onnxruntime_USE_TENSORRT_BUILTIN_PARSER':'OFF',
'onnxruntime_TENSORRT_PLACEHOLDER_BUILDER':'OFF', 'onnxruntime_USE_TVM':'OFF',
'onnxruntime_TVM_CUDA_RUNTIME':'OFF', 'onnxruntime_TVM_USE_HASH':'OFF',
'onnxruntime_USE_MIGRAPHX':'OFF', 'onnxruntime_CROSS_COMPILING':'OFF',
'onnxruntime_DISABLE_CONTRIB_OPS':'OFF', 'onnxruntime_DISABLE_ML_OPS':'OFF',
'onnxruntime_DISABLE_RTTI':'OFF', 'onnxruntime_DISABLE_EXCEPTIONS':'OFF',
'onnxruntime_MINIMAL_BUILD':'OFF', 'onnxruntime_EXTENDED_MINIMAL_BUILD':'OFF',
'onnxruntime_MINIMAL_BUILD_CUSTOM_OPS':'OFF', 'onnxruntime_REDUCED_OPS_BUILD':'OFF',
'onnxruntime_ENABLE_LANGUAGE_INTEROP_OPS':'OFF', 'onnxruntime_USE_DML':'OFF',
'onnxruntime_USE_WINML':'OFF', 'onnxruntime_BUILD_MS_EXPERIMENTAL_OPS':'OFF',
'onnxruntime_USE_TELEMETRY':'OFF', 'onnxruntime_ENABLE_LTO':'OFF',
'onnxruntime_USE_ACL':'OFF', 'onnxruntime_USE_ACL_1902':'OFF',
'onnxruntime_USE_ACL_1905':'OFF', 'onnxruntime_USE_ACL_1908':'OFF',
'onnxruntime_USE_ACL_2002':'OFF', 'onnxruntime_USE_ARMNN':'OFF',
'onnxruntime_ARMNN_RELU_USE_CPU':'ON', 'onnxruntime_ARMNN_BN_USE_CPU':'ON',
'onnxruntime_ENABLE_NVTX_PROFILE':'OFF', 'onnxruntime_ENABLE_TRAINING':'OFF',
'onnxruntime_ENABLE_TRAINING_OPS':'OFF', 'onnxruntime_ENABLE_TRAINING_APIS':'OFF',
'onnxruntime_ENABLE_CPU_FP16_OPS':'OFF', 'onnxruntime_USE_NCCL':'OFF',
'onnxruntime_BUILD_BENCHMARKS':'OFF', 'onnxruntime_USE_ROCM':'OFF',
'Onnxruntime_GCOV_COVERAGE':'OFF', 'onnxruntime_USE_MPI':'ON',
'onnxruntime_ENABLE_MEMORY_PROFILE':'OFF',
'onnxruntime_ENABLE_CUDA_LINE_NUMBER_INFO':'OFF',
'onnxruntime_BUILD_WEBASSEMBLY':'OFF', 'onnxruntime_BUILD_WEBASSEMBLY_STATIC_LIB':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_EXCEPTION_CATCHING':'ON',
'onnxruntime_ENABLE_WEBASSEMBLY_API_EXCEPTION_CATCHING':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_EXCEPTION_THROWING':'ON',
'onnxruntime_ENABLE_WEBASSEMBLY_THREADS':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_DEBUG_INFO':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_PROFILING':'OFF',
'onnxruntime_ENABLE_EAGER_MODE':'OFF', 'onnxruntime_ENABLE_LAZY_TENSOR':'OFF',
'onnxruntime_ENABLE_EXTERNAL_CUSTOM_OP_SCHEMAS':'OFF', 'onnxruntime_ENABLE_CUDA_PROFILING':'OFF',
'onnxruntime_ENABLE_ROCM_PROFILING':'OFF', 'onnxruntime_USE_XNNPACK':'OFF',
'onnxruntime_USE_CANN':'OFF', 'CMAKE_TLS_VERIFY':'ON', 'FETCHCONTENT_QUIET':'OFF',
'onnxruntime_PYBIND_EXPORT_OPSCHEMA':'OFF', 'onnxruntime_ENABLE_MEMLEAK_CHECKER':'OFF',
'CMAKE_BUILD_TYPE':'Release',
'onnxruntime_USE_EXTENSIONS': 'ON',
'onnxruntime_EXTENSIONS_PATH': '$EXT_BUILD_ROOT/external/onnx_runtime/cmake/external/onnxruntime-extensions',
'OCOS_ENABLE_BLINGFIRE': 'OFF'
}


__ONNXRUNTIME_WITH_CUDA = {'onnxruntime_RUN_ONNX_TESTS':'OFF',
'onnxruntime_GENERATE_TEST_REPORTS':'ON',
'onnxruntime_USE_MIMALLOC':'OFF',
'onnxruntime_ENABLE_PYTHON':'OFF',
'onnxruntime_BUILD_CSHARP':'OFF',
'onnxruntime_BUILD_JAVA':'OFF',
'onnxruntime_BUILD_NODEJS':'OFF',
'onnxruntime_BUILD_OBJC':'OFF',
'onnxruntime_BUILD_SHARED_LIB':'OFF',
'onnxruntime_BUILD_APPLE_FRAMEWORK':'OFF',
'onnxruntime_USE_DNNL':'OFF',
'onnxruntime_USE_NNAPI_BUILTIN':'OFF',
'onnxruntime_USE_RKNPU':'OFF',
'onnxruntime_USE_LLVM':'OFF',
'onnxruntime_ENABLE_MICROSOFT_INTERNAL':'OFF',
'onnxruntime_USE_VITISAI':'OFF',
'onnxruntime_USE_TENSORRT':'OFF',
'onnxruntime_SKIP_AND_PERFORM_FILTERED_TENSORRT_TESTS':'ON',
'onnxruntime_USE_TENSORRT_BUILTIN_PARSER':'OFF',
'onnxruntime_TENSORRT_PLACEHOLDER_BUILDER':'OFF', 'onnxruntime_USE_TVM':'OFF',
'onnxruntime_TVM_CUDA_RUNTIME':'OFF', 'onnxruntime_TVM_USE_HASH':'OFF',
'onnxruntime_USE_MIGRAPHX':'OFF', 'onnxruntime_CROSS_COMPILING':'OFF',
'onnxruntime_DISABLE_CONTRIB_OPS':'OFF', 'onnxruntime_DISABLE_ML_OPS':'OFF',
'onnxruntime_DISABLE_RTTI':'OFF', 'onnxruntime_DISABLE_EXCEPTIONS':'OFF',
'onnxruntime_MINIMAL_BUILD':'OFF', 'onnxruntime_EXTENDED_MINIMAL_BUILD':'OFF',
'onnxruntime_MINIMAL_BUILD_CUSTOM_OPS':'OFF', 'onnxruntime_REDUCED_OPS_BUILD':'OFF',
'onnxruntime_ENABLE_LANGUAGE_INTEROP_OPS':'OFF', 'onnxruntime_USE_DML':'OFF',
'onnxruntime_USE_WINML':'OFF', 'onnxruntime_BUILD_MS_EXPERIMENTAL_OPS':'OFF',
'onnxruntime_USE_TELEMETRY':'OFF', 'onnxruntime_ENABLE_LTO':'OFF',
'onnxruntime_USE_ACL':'OFF', 'onnxruntime_USE_ACL_1902':'OFF',
'onnxruntime_USE_ACL_1905':'OFF', 'onnxruntime_USE_ACL_1908':'OFF',
'onnxruntime_USE_ACL_2002':'OFF', 'onnxruntime_USE_ARMNN':'OFF',
'onnxruntime_ARMNN_RELU_USE_CPU':'ON', 'onnxruntime_ARMNN_BN_USE_CPU':'ON',
'onnxruntime_ENABLE_NVTX_PROFILE':'OFF', 'onnxruntime_ENABLE_TRAINING':'OFF',
'onnxruntime_ENABLE_TRAINING_OPS':'OFF', 'onnxruntime_ENABLE_TRAINING_APIS':'OFF',
'onnxruntime_ENABLE_CPU_FP16_OPS':'OFF', 'onnxruntime_USE_NCCL':'OFF',
'onnxruntime_BUILD_BENCHMARKS':'OFF', 'onnxruntime_USE_ROCM':'OFF',
'Onnxruntime_GCOV_COVERAGE':'OFF', 'onnxruntime_USE_MPI':'ON',
'onnxruntime_ENABLE_MEMORY_PROFILE':'OFF',
'onnxruntime_ENABLE_CUDA_LINE_NUMBER_INFO':'OFF',
'onnxruntime_BUILD_WEBASSEMBLY':'OFF', 'onnxruntime_BUILD_WEBASSEMBLY_STATIC_LIB':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_EXCEPTION_CATCHING':'ON',
'onnxruntime_ENABLE_WEBASSEMBLY_API_EXCEPTION_CATCHING':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_EXCEPTION_THROWING':'ON',
'onnxruntime_ENABLE_WEBASSEMBLY_THREADS':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_DEBUG_INFO':'OFF',
'onnxruntime_ENABLE_WEBASSEMBLY_PROFILING':'OFF',
'onnxruntime_ENABLE_EAGER_MODE':'OFF', 'onnxruntime_ENABLE_LAZY_TENSOR':'OFF',
'onnxruntime_ENABLE_EXTERNAL_CUSTOM_OP_SCHEMAS':'OFF', 'onnxruntime_ENABLE_CUDA_PROFILING':'OFF',
'onnxruntime_ENABLE_ROCM_PROFILING':'OFF', 'onnxruntime_USE_XNNPACK':'OFF',
'onnxruntime_USE_CANN':'OFF', 'CMAKE_TLS_VERIFY':'ON', 'FETCHCONTENT_QUIET':'OFF',
'onnxruntime_PYBIND_EXPORT_OPSCHEMA':'OFF', 'onnxruntime_ENABLE_MEMLEAK_CHECKER':'OFF',
'CMAKE_BUILD_TYPE':'Release', 'onnxruntime_USE_CUDA':'ON', 'onnxruntime_USE_CUDNN':'ON',
'onnxruntime_USE_EXTENSIONS': 'ON',
'onnxruntime_EXTENSIONS_PATH': '$EXT_BUILD_ROOT/external/onnx_runtime/cmake/external/onnxruntime-extensions',
'onnxruntime_CUDA_HOME': CUDA_HOME,
'onnxruntime_CUDNN_HOME': CUDNN_HOME,
'CMAKE_CUDA_COMPILER': CUDA_HOME + "/bin/nvcc",
'OCOS_ENABLE_BLINGFIRE': 'OFF'
}

config_setting(
    name = "with_cuda",
    define_values = { "use_cuda": "on" }
)



cmake(
    name = "onnxruntime",
    lib_source = "//:all_srcs",
    cache_entries = select({
        ":with_cuda":   __ONNXRUNTIME_WITH_CUDA,
        "//conditions:default": __ONNXRUNTIME_WITHOUT_CUDA
    }),
    working_directory="cmake",
    build_args= [
        "--config Release",
        "-j3"
    ],
    tags=["requires-network","no-sandbox"],
    features=["-default_compile_flags","-fno-canonical-system-headers", "-Wno-builtin-macro-redefined"],
    out_static_libs=[ "libonnxruntime_session.a",
    "libonnxruntime_optimizer.a",
    "libonnxruntime_providers.a",
    "libonnxruntime_util.a",
    "libonnxruntime_framework.a",
    "libonnxruntime_graph.a",
    "libonnxruntime_mlas.a",
    "libonnxruntime_common.a",
    "libonnxruntime_flatbuffers.a",
    "libortcustomops.a",
    "libocos_operators.a",
    "libnoexcep_operators.a",
    "_deps/onnx-build/libonnx.a",
    "_deps/onnx-build/libonnx_proto.a",
    "_deps/re2-build/libre2.a",
    "_deps/abseil_cpp-build/absl/base/libabsl_base.a",
    "_deps/abseil_cpp-build/absl/base/libabsl_throw_delegate.a",
    "_deps/abseil_cpp-build/absl/container/libabsl_raw_hash_set.a",
    "_deps/abseil_cpp-build/absl/hash/libabsl_hash.a",
    "_deps/abseil_cpp-build/absl/hash/libabsl_city.a",
    "_deps/abseil_cpp-build/absl/hash/libabsl_low_level_hash.a",
    "_deps/google_nsync-build/libnsync_cpp.a",
    "_deps/pytorch_cpuinfo-build/libcpuinfo.a",
    "_deps/pytorch_cpuinfo-build/deps/clog/libclog.a",
    "_deps/opencv-build/lib/libopencv_imgcodecs.a",
    "_deps/opencv-build/lib/libopencv_imgproc.a",
    "_deps/opencv-build/lib/libopencv_core.a",
    "_deps/opencv-build/3rdparty/lib/liblibjpeg-turbo.a",
    "_deps/opencv-build/3rdparty/lib/liblibpng.a"
    ],
    postfix_script= select({
        ":with_cuda": __POSTFIX_WITH_CUDA,
        "//conditions:default": __POSTFIX
    }),
)

cc_library(
    name = "onnxruntime_lib",
    linkopts = select({
        "@platforms//os:linux": ["-static-libstdc++", "-static-libgcc"],
        "//conditions:default": [],
    }),
    deps = ["//:onnxruntime", "@onnx_runtime_extensions//:operators"],
    includes= ["onnxruntime/include/onnxruntime"],
    visibility = ["//visibility:public"]
)
