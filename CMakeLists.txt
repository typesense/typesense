cmake_minimum_required(VERSION 3.24.0)
project(typesense)

cmake_policy(SET CMP0074 NEW)
cmake_policy(SET CMP0003 NEW)

set(USE_SANTINIZER OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -Wextra -Wno-unused-parameter -Werror=return-type -O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -Wno-unused-parameter -Werror=return-type -std=c++17 -O0 -g")
set(DEP_ROOT_DIR ${CMAKE_SOURCE_DIR}/external-${CMAKE_SYSTEM_NAME})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-class-memaccess")
else()
    if(USE_SANTINIZER)
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread -fsanitize-recover=all -fPIE")
    endif()
endif()

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -mcpu=native")
endif()

# will be overridden from command line during build
set(TYPESENSE_VERSION "nightly" CACHE STRING "")
set(BUILD_DEPS "yes" CACHE STRING "")

if(NOT EXISTS ${DEP_ROOT_DIR})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR})
endif()

# Needed to enable Snappy on RocksDB
add_definitions(-DSNAPPY)
add_definitions(-DS2_USE_GLOG)

if (APPLE)
    add_definitions(-DGTEST_USE_OWN_TR1_TUPLE)
    add_definitions(-D__GLIBCXX__)

    # Prefer brew installated libraries
    set(OPENSSL_ROOT_DIR /usr/local/opt/openssl@1.1)
    set(ENV{OPENSSL_ROOT_DIR} /usr/local/opt/openssl@1.1)

    set(SNAPPY_ROOT_DIR /usr/local/opt/snappy)
    set(ZLIB_ROOT /usr/local/opt/zlib)
    set(CMAKE_PREFIX_PATH /usr/local/opt/curl-openssl /usr/local/opt/icu4c)

    add_compile_definitions(USE_BACKWARD=1)
else()
    add_compile_definitions(BACKWARD_HAS_DW=1)
endif()

# Ensure that we only use the static versions of libraries to link against
# You can't statically link a shared library (or dynamically link a static library)
IF(WIN32 OR MSVC)
    SET(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a)
ELSE()
    SET(CMAKE_FIND_LIBRARY_SUFFIXES .a)
    set(ENV{CMAKE_FIND_LIBRARY_SUFFIXES} ".a")
ENDIF()

include(cmake/onnxruntime.cmake)
include(cmake/onnxruntime_ext.cmake)
include(cmake/For.cmake)
include(cmake/H2O.cmake)
include(cmake/RocksDB.cmake)
include(cmake/GoogleTest.cmake)
include(cmake/TestResources.cmake)
include(cmake/Iconv.cmake)
include(cmake/Jemalloc.cmake)
include(cmake/s2.cmake)
include(cmake/lrucache.cmake)
include(cmake/kakasi.cmake)
include(cmake/hnsw.cmake)

FIND_PACKAGE(OpenSSL 1.1.1 REQUIRED)
FIND_PACKAGE(Snappy REQUIRED)
FIND_PACKAGE(ZLIB REQUIRED)
FIND_PACKAGE(CURL REQUIRED)
FIND_PACKAGE(ICU REQUIRED)
FIND_PACKAGE(Protobuf REQUIRED)
FIND_PACKAGE(LevelDB REQUIRED)
FIND_PACKAGE(gflags REQUIRED)
FIND_PACKAGE(glog REQUIRED)

message("OpenSSL library: ${OPENSSL_LIBRARIES}")

FILE(GLOB SRC_FILES src/*.cpp ${DEP_ROOT_DIR}/${KAKASI_NAME}/data/*.cpp)
FILE(GLOB TEST_FILES test/*.cpp)

include_directories(include)
include_directories(include/tsl)
include_directories(/usr/local/include)
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${CURL_INCLUDE_DIR})
include_directories(${ICU_INCLUDE_DIRS})
include_directories(${DEP_ROOT_DIR}/${FOR_NAME})
include_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/include)
include_directories(${DEP_ROOT_DIR}/${H2O_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${H2O_NAME}/include/h2o)
include_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${ICONV_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${BRPC_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${BRAFT_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${JEMALLOC_NAME}/include/jemalloc)
include_directories(${DEP_ROOT_DIR}/${S2_NAME}/src)
include_directories(${DEP_ROOT_DIR}/${LRUCACHE_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${KAKASI_NAME}/build/include)
include_directories(${DEP_ROOT_DIR}/${KAKASI_NAME}/data)
include_directories(${DEP_ROOT_DIR}/${HNSW_NAME})
include_directories(${DEP_ROOT_DIR}/${ONNX_NAME}/include/onnxruntime)
include_directories(${DEP_ROOT_DIR}/${ONNX_EXT_NAME}/operators/src_dir)


link_directories(/usr/local/lib)
link_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/build)
link_directories(${DEP_ROOT_DIR}/${FOR_NAME})
link_directories(${DEP_ROOT_DIR}/${H2O_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME})
link_directories(${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs)
link_directories(${DEP_ROOT_DIR}/${JEMALLOC_NAME}/lib)
link_directories(${DEP_ROOT_DIR}/${S2_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${KAKASI_NAME}/build/lib)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/onnx-build)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/re2-build)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/abseil_cpp-build)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/abseil_cpp-build/absl)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/abseil_cpp-build/absl/base)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/protobuf-build)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/abseil_cpp-build/absl/container)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/abseil_cpp-build/absl/hash)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/pytorch_cpuinfo-build)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/pytorch_cpuinfo-build/deps)
link_directories(${DEP_ROOT_DIR}/${ONNX_NAME}-build/lib/_deps/pytorch_cpuinfo-build/deps/clog)

set(JEMALLOC_ROOT_DIR "${DEP_ROOT_DIR}/${JEMALLOC_NAME}")
FIND_PACKAGE(Jemalloc REQUIRED)

add_executable(typesense-server ${SRC_FILES} src/main/typesense_server.cpp)
add_executable(search ${SRC_FILES} src/main/main.cpp)
add_executable(benchmark ${SRC_FILES} src/main/benchmark.cpp)
add_executable(typesense-test ${SRC_FILES} ${TEST_FILES})

add_library(ONNX_SESSION IMPORTED STATIC)
set_target_properties(ONNX_SESSION PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_session.a)
add_library(ONNX_OPT STATIC IMPORTED)
set_target_properties(ONNX_OPT PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_optimizer.a)
add_library(ONNX_PRO STATIC IMPORTED)
set_target_properties(ONNX_PRO PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_providers.a)
add_library(ONNX_UTL STATIC IMPORTED)
set_target_properties(ONNX_UTL PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_util.a)
add_library(ONNX_FRM STATIC IMPORTED)
set_target_properties(ONNX_FRM PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_framework.a)
add_library(ONNX_GRP STATIC IMPORTED)
set_target_properties(ONNX_GRP PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_graph.a)
add_library(ONNX_MLS STATIC IMPORTED)
set_target_properties(ONNX_MLS PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_mlas.a)
add_library(ONNX_CMN STATIC IMPORTED)
set_target_properties(ONNX_CMN PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_common.a)
add_library(ONNX_FLT STATIC IMPORTED)
set_target_properties(ONNX_FLT PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/libonnxruntime_flatbuffers.a)
add_library(ONNX STATIC IMPORTED)
set_target_properties(ONNX PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/onnx-build/libonnx.a)
add_library(ONNX_PRT STATIC IMPORTED)
set_target_properties(ONNX_PRT PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/onnx-build/libonnx_proto.a)
add_library(ONNX_PRTL STATIC IMPORTED)
set_target_properties(ONNX_PRTL PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/protobuf-build/libprotobuf-lite.a)
add_library(ONNX_RE STATIC IMPORTED)
set_target_properties(ONNX_RE PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/re2-build/libre2.a)
add_library(ABSL STATIC IMPORTED)
set_target_properties(ABSL PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/abseil_cpp-build/absl/base/libabsl_base.a)
add_library(ABSL_DEL STATIC IMPORTED)
set_target_properties(ABSL_DEL PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/abseil_cpp-build/absl/base/libabsl_throw_delegate.a)
add_library(ABSL_RW STATIC IMPORTED)
set_target_properties(ABSL_RW PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/abseil_cpp-build/absl/container/libabsl_raw_hash_set.a)
add_library(ABSL_HSH STATIC IMPORTED)
set_target_properties(ABSL_HSH PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/abseil_cpp-build/absl/hash/libabsl_hash.a)
add_library(ABSL_CTY STATIC IMPORTED)
set_target_properties(ABSL_CTY PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/abseil_cpp-build/absl/hash/libabsl_city.a)
add_library(ABSL_LL STATIC IMPORTED)
set_target_properties(ABSL_LL PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/abseil_cpp-build/absl/hash/libabsl_low_level_hash.a)
add_library(NSYNC STATIC IMPORTED)
set_target_properties(NSYNC PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/google_nsync-build/libnsync_cpp.a)
add_library(CPUI STATIC IMPORTED)
set_target_properties(CPUI PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/pytorch_cpuinfo-build/libcpuinfo.a)
add_library(CLOG STATIC IMPORTED)
set_target_properties(CLOG PROPERTIES IMPORTED_LOCATION ${DEP_ROOT_DIR}/${ONNX_NAME}-build/_deps/pytorch_cpuinfo-build/deps/clog/libclog.a)

target_compile_definitions(
    typesense-server PRIVATE
    TYPESENSE_VERSION="${TYPESENSE_VERSION}"
)

target_compile_definitions(
    benchmark PRIVATE
    TYPESENSE_VERSION="${TYPESENSE_VERSION}"
)

target_compile_definitions(
    search PRIVATE
    TYPESENSE_VERSION="${TYPESENSE_VERSION}"
)

target_compile_definitions(
    typesense-test PRIVATE
    ROOT_DIR="${CMAKE_SOURCE_DIR}/"
    TYPESENSE_VERSION="${TYPESENSE_VERSION}"
)

set(ROCKSDB_LIBS rocksdb ${SNAPPY_LIBRARIES})
set(STACKTRACE_LIBS "")

if (APPLE)
    set(STD_LIB "") # https://stackoverflow.com/a/26543140/131050 (can't statically link libgcc on Mac)

    FIND_PACKAGE(ngHTTP2 REQUIRED)
    set(CURL_LIBRARIES ${NGHTTP2_LIBRARIES} ${CURL_LIBRARIES}) # nghttp2 was not being statically built
    set(SYSTEM_LIBS "-framework Foundation" "-framework SystemConfiguration" "-framework CoreText"
            "-framework CoreGraphics" "-framework Security" "-framework Foundation"
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory" "-Wl,-U,_ProfilerStart" "-Wl,-U,_ProfilerStop")
else()
    set(STD_LIB -static-libgcc -static-libstdc++)
    list(APPEND STACKTRACE_LIBS dw elf)
    list(APPEND SYSTEM_LIBS rt)
endif()

set(ICU_ALL_LIBRARIES ${ICU_I18N_LIBRARIES} ${ICU_LIBRARIES} ${ICU_DATA_LIBRARIES})
set(CORE_LIBS kakasi h2o-evloop braft brpc iconv ${ICU_ALL_LIBRARIES} ${CURL_LIBRARIES} for s2
              ${LevelDB_LIBRARIES} ${ROCKSDB_LIBS}
              glog ${GFLAGS_LIBRARIES} ${PROTOBUF_LIBRARIES} ${STACKTRACE_LIBS}
              ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${JEMALLOC_LIBRARIES}
              ${SYSTEM_LIBS} pthread dl ${STD_LIB} ONNX_SESSION ONNX_OPT ONNX_PRO ONNX_UTL ONNX_FRM ONNX_GRP ONNX_MLS ONNX_CMN ONNX_FLT ONNX ONNX_PRT ONNX_PRTL ONNX_RE ABSL ABSL_DEL ABSL_RW ABSL_HSH ABSL_CTY ABSL_LL NSYNC CPUI CLOG)

target_link_libraries(typesense-server ${CORE_LIBS})
target_link_libraries(search ${CORE_LIBS})
target_link_libraries(benchmark ${CORE_LIBS})
target_link_libraries(typesense-test ${CORE_LIBS} gtest gtest_main)

add_dependencies(typesense-server onnxruntime)
add_dependencies(typesense-test onnxruntime)
add_dependencies(benchmark onnxruntime)
add_dependencies(search onnxruntime)

# add source files from ${DEP_ROOT_DIR}/${ONNX_EXT_NAME} directory to targets
set(ONNX_EXT_SRC_FILES ${DEP_ROOT_DIR}/${ONNX_EXT_NAME}/operators/src_dir/ustring.cc ${DEP_ROOT_DIR}/${ONNX_EXT_NAME}/operators/src_dir/string_utils_onnx.cc ${DEP_ROOT_DIR}/${ONNX_EXT_NAME}/operators/src_dir/base64.cc ${DEP_ROOT_DIR}/${ONNX_EXT_NAME}/operators/src_dir/tokenizer/bert_tokenizer.cc ${DEP_ROOT_DIR}/${ONNX_EXT_NAME}/operators/src_dir/tokenizer/basic_tokenizer.cc) 
set_source_files_properties(${ONNX_EXT_SRC_FILES} PROPERTIES GENERATED TRUE)
target_sources(typesense-server PRIVATE ${ONNX_EXT_SRC_FILES})
target_sources(typesense-test PRIVATE ${ONNX_EXT_SRC_FILES})
target_sources(benchmark PRIVATE ${ONNX_EXT_SRC_FILES})
target_sources(search PRIVATE ${ONNX_EXT_SRC_FILES})

add_dependencies(typesense-server onnxruntime_ext)
add_dependencies(typesense-test onnxruntime_ext)
add_dependencies(benchmark onnxruntime_ext)
add_dependencies(search onnxruntime_ext)
