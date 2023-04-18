project(onnxruntme)
set(ONNX_NAME onnxruntime)
include(ExternalProject)

if(NOT EXISTS ${DEP_ROOT_DIR}/${ONNX_NAME})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${ONNX_NAME})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${ONNX_NAME}-build)
endif()

ExternalProject_Add(
    onnxruntime
    GIT_REPOSITORY https://github.com/microsoft/onnxruntime
    GIT_TAG origin/rel-1.14.0
    SOURCE_DIR ${DEP_ROOT_DIR}/${ONNX_NAME}
    PATCH_COMMAND cd ${DEP_ROOT_DIR}/${ONNX_NAME} && git apply ${CMAKE_CURRENT_SOURCE_DIR}/cmake/onnx.patch || git apply ${CMAKE_CURRENT_SOURCE_DIR}/cmake/onnx.patch -R --check && git submodule sync && git submodule foreach  'git fetch --tags' && git submodule update --init --remote
    BINARY_DIR ${DEP_ROOT_DIR}/${ONNX_NAME}-build
    CONFIGURE_COMMAND ${CMAKE_COMMAND} ${DEP_ROOT_DIR}/${ONNX_NAME}/cmake -B${DEP_ROOT_DIR}/${ONNX_NAME}-build -Donnxruntime_RUN_ONNX_TESTS=OFF -Donnxruntime_GENERATE_TEST_REPORTS=ON -Donnxruntime_USE_MIMALLOC=OFF -Donnxruntime_ENABLE_PYTHON=OFF -Donnxruntime_BUILD_CSHARP=OFF -Donnxruntime_BUILD_JAVA=OFF -Donnxruntime_BUILD_NODEJS=OFF -Donnxruntime_BUILD_OBJC=OFF -Donnxruntime_BUILD_SHARED_LIB=OFF -Donnxruntime_BUILD_APPLE_FRAMEWORK=OFF -Donnxruntime_USE_DNNL=OFF -Donnxruntime_USE_NNAPI_BUILTIN=OFF -Donnxruntime_USE_RKNPU=OFF -Donnxruntime_USE_LLVM=OFF -Donnxruntime_ENABLE_MICROSOFT_INTERNAL=OFF -Donnxruntime_USE_VITISAI=OFF -Donnxruntime_USE_TENSORRT=OFF -Donnxruntime_SKIP_AND_PERFORM_FILTERED_TENSORRT_TESTS=ON -Donnxruntime_USE_TENSORRT_BUILTIN_PARSER=OFF -Donnxruntime_TENSORRT_PLACEHOLDER_BUILDER=OFF -Donnxruntime_USE_TVM=OFF -Donnxruntime_TVM_CUDA_RUNTIME=OFF -Donnxruntime_TVM_USE_HASH=OFF -Donnxruntime_USE_MIGRAPHX=OFF -Donnxruntime_CROSS_COMPILING=OFF -Donnxruntime_DISABLE_CONTRIB_OPS=OFF -Donnxruntime_DISABLE_ML_OPS=OFF -Donnxruntime_DISABLE_RTTI=OFF -Donnxruntime_DISABLE_EXCEPTIONS=OFF -Donnxruntime_MINIMAL_BUILD=OFF -Donnxruntime_EXTENDED_MINIMAL_BUILD=OFF -Donnxruntime_MINIMAL_BUILD_CUSTOM_OPS=OFF -Donnxruntime_REDUCED_OPS_BUILD=OFF -Donnxruntime_ENABLE_LANGUAGE_INTEROP_OPS=OFF -Donnxruntime_USE_DML=OFF -Donnxruntime_USE_WINML=OFF -Donnxruntime_BUILD_MS_EXPERIMENTAL_OPS=OFF -Donnxruntime_USE_TELEMETRY=OFF -Donnxruntime_ENABLE_LTO=OFF -Donnxruntime_USE_ACL=OFF -Donnxruntime_USE_ACL_1902=OFF -Donnxruntime_USE_ACL_1905=OFF -Donnxruntime_USE_ACL_1908=OFF -Donnxruntime_USE_ACL_2002=OFF -Donnxruntime_USE_ARMNN=OFF -Donnxruntime_ARMNN_RELU_USE_CPU=ON -Donnxruntime_ARMNN_BN_USE_CPU=ON -Donnxruntime_ENABLE_NVTX_PROFILE=OFF -Donnxruntime_ENABLE_TRAINING=OFF -Donnxruntime_ENABLE_TRAINING_OPS=OFF -Donnxruntime_ENABLE_TRAINING_APIS=OFF -Donnxruntime_ENABLE_CPU_FP16_OPS=OFF -Donnxruntime_USE_NCCL=OFF -Donnxruntime_BUILD_BENCHMARKS=OFF -Donnxruntime_USE_ROCM=OFF -Donnxruntime_GCOV_COVERAGE=OFF -Donnxruntime_USE_MPI=ON -Donnxruntime_ENABLE_MEMORY_PROFILE=OFF -Donnxruntime_ENABLE_CUDA_LINE_NUMBER_INFO=OFF -Donnxruntime_BUILD_WEBASSEMBLY=OFF -Donnxruntime_BUILD_WEBASSEMBLY_STATIC_LIB=OFF -Donnxruntime_ENABLE_WEBASSEMBLY_EXCEPTION_CATCHING=ON -Donnxruntime_ENABLE_WEBASSEMBLY_API_EXCEPTION_CATCHING=OFF -Donnxruntime_ENABLE_WEBASSEMBLY_EXCEPTION_THROWING=ON -Donnxruntime_ENABLE_WEBASSEMBLY_THREADS=OFF -Donnxruntime_ENABLE_WEBASSEMBLY_DEBUG_INFO=OFF -Donnxruntime_ENABLE_WEBASSEMBLY_PROFILING=OFF -Donnxruntime_ENABLE_EAGER_MODE=OFF -Donnxruntime_ENABLE_LAZY_TENSOR=OFF -Donnxruntime_ENABLE_EXTERNAL_CUSTOM_OP_SCHEMAS=OFF -Donnxruntime_ENABLE_CUDA_PROFILING=OFF -Donnxruntime_ENABLE_ROCM_PROFILING=OFF -Donnxruntime_USE_XNNPACK=OFF -Donnxruntime_USE_CANN=OFF -DCMAKE_TLS_VERIFY=ON -DFETCHCONTENT_QUIET=OFF -Donnxruntime_PYBIND_EXPORT_OPSCHEMA=OFF -Donnxruntime_ENABLE_MEMLEAK_CHECKER=OFF -DCMAKE_BUILD_TYPE=Release 
    BUILD_COMMAND ${CMAKE_COMMAND} --build ${DEP_ROOT_DIR}/${ONNX_NAME}-build --config Release -- -j8
)
