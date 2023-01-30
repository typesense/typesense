cmake_minimum_required(VERSION 3.17.5)
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

link_directories(/usr/local/lib)
link_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/build)
link_directories(${DEP_ROOT_DIR}/${FOR_NAME})
link_directories(${DEP_ROOT_DIR}/${H2O_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME})
link_directories(${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs)
link_directories(${DEP_ROOT_DIR}/${JEMALLOC_NAME}/lib)
link_directories(${DEP_ROOT_DIR}/${S2_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${KAKASI_NAME}/build/lib)

set(JEMALLOC_ROOT_DIR "${DEP_ROOT_DIR}/${JEMALLOC_NAME}")
FIND_PACKAGE(Jemalloc REQUIRED)

add_executable(typesense-server ${SRC_FILES} src/main/typesense_server.cpp)
add_executable(search ${SRC_FILES} src/main/main.cpp)
add_executable(benchmark ${SRC_FILES} src/main/benchmark.cpp)
add_executable(typesense-test ${SRC_FILES} ${TEST_FILES})

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
    set(SYSTEM_LIBS "-framework CoreFoundation" "-framework CoreGraphics" "-framework CoreData"
            "-framework SystemConfiguration" "-framework CoreText" "-framework Security" "-framework Foundation"
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
              ${SYSTEM_LIBS} pthread dl ${STD_LIB})

target_link_libraries(typesense-server ${CORE_LIBS})
target_link_libraries(search ${CORE_LIBS})
target_link_libraries(benchmark ${CORE_LIBS})
target_link_libraries(typesense-test ${CORE_LIBS} gtest gtest_main)
