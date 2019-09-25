cmake_minimum_required(VERSION 2.8)
project(typesense)

cmake_policy(SET CMP0074 NEW)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wextra -Wno-unused-parameter -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -Wno-unused-parameter -std=c++11 -O0")
set(DEP_ROOT_DIR ${CMAKE_SOURCE_DIR}/external-${CMAKE_SYSTEM_NAME})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# will be overridden from command line during build
set(TYPESENSE_VERSION "nightly" CACHE STRING "")
set(BUILD_DEPS "yes" CACHE STRING "")

if(NOT EXISTS ${DEP_ROOT_DIR})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR})
endif()

if (APPLE)
    add_definitions(-DGTEST_USE_OWN_TR1_TUPLE)
    add_definitions(-D__GLIBCXX__)

    # Prefer brew installated libraries
    set(OPENSSL_ROOT_DIR /usr/local/opt/openssl@1.1)
    set(SNAPPY_ROOT_DIR /usr/local/opt/snappy)
    set(ZLIB_ROOT /usr/local/opt/zlib)
    set(CMAKE_PREFIX_PATH /usr/local/opt/curl-openssl /usr/local/opt/icu4c)
endif (APPLE)

# Ensure that we only use the static versions of libraries to link against
# You can't statically link a shared library (or dynamically link a static library)
IF(WIN32 OR MSVC)
    SET(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a)
ELSE()
    SET(CMAKE_FIND_LIBRARY_SUFFIXES .a)
ENDIF()

FIND_PACKAGE(OpenSSL 1.1.1 REQUIRED)
FIND_PACKAGE(Snappy REQUIRED)
FIND_PACKAGE(ZLIB REQUIRED)
FIND_PACKAGE(CURL REQUIRED)
FIND_PACKAGE(ICU REQUIRED)

message("OpenSSL library: ${OPENSSL_LIBRARIES}")

include(cmake/For.cmake)
include(cmake/H2O.cmake)
include(cmake/RocksDB.cmake)
include(cmake/GoogleTest.cmake)
include(cmake/TestResources.cmake)
include(cmake/g3log.cmake)
include(cmake/Iconv.cmake)

FILE(GLOB SRC_FILES src/*.cpp)

include_directories(include)
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${CURL_INCLUDE_DIR})
include_directories(${ICU_INCLUDE_DIRS})
include_directories(${DEP_ROOT_DIR}/${FOR_NAME})
include_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/include)
include_directories(${DEP_ROOT_DIR}/${H2O_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${G3LOG_NAME}/src)
include_directories(${DEP_ROOT_DIR}/${G3LOG_NAME}/build/include)
include_directories(${DEP_ROOT_DIR}/${ICONV_NAME}/include)

# Write dependency include directories to a file
file(WRITE ${DEP_ROOT_DIR}/includes.txt "")
file(APPEND ${DEP_ROOT_DIR}/includes.txt "${DEP_ROOT_DIR}/${FOR_NAME}")
file(APPEND ${DEP_ROOT_DIR}/includes.txt " ${DEP_ROOT_DIR}/${H2O_NAME}/include")
file(APPEND ${DEP_ROOT_DIR}/includes.txt " ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/include")
file(APPEND ${DEP_ROOT_DIR}/includes.txt " ${DEP_ROOT_DIR}/${G3LOG_NAME}/src")
file(APPEND ${DEP_ROOT_DIR}/includes.txt " ${DEP_ROOT_DIR}/${G3LOG_NAME}/build/include")

link_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/build)
link_directories(${DEP_ROOT_DIR}/${FOR_NAME})
link_directories(${DEP_ROOT_DIR}/${H2O_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME})
link_directories(${DEP_ROOT_DIR}/${G3LOG_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs)

# Write dependency libraries to a file
file(WRITE ${DEP_ROOT_DIR}/libs.txt "")
file(APPEND ${DEP_ROOT_DIR}/libs.txt " ${DEP_ROOT_DIR}/${FOR_NAME}")
file(APPEND ${DEP_ROOT_DIR}/libs.txt " ${DEP_ROOT_DIR}/${H2O_NAME}/build")
file(APPEND ${DEP_ROOT_DIR}/libs.txt " ${DEP_ROOT_DIR}/${ROCKSDB_NAME}")
file(APPEND ${DEP_ROOT_DIR}/libs.txt " ${DEP_ROOT_DIR}/${G3LOG_NAME}/build")
file(APPEND ${DEP_ROOT_DIR}/libs.txt " ${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs")

add_executable(typesense-server ${SRC_FILES} src/main/typesense_server.cpp)
add_executable(search ${SRC_FILES} src/main/main.cpp)
add_executable(benchmark ${SRC_FILES} src/main/benchmark.cpp)
add_executable(typesense-test ${SRC_FILES} test/main.cpp test/array_test.cpp test/sorted_array_test.cpp test/art_test.cpp
               test/collection_test.cpp test/collection_manager_test.cpp
               test/topster_test.cpp test/match_score_test.cpp test/store_test.cpp test/array_utils_test.cpp
               test/string_utils_test.cpp test/config_test.cpp)

add_library(typesense-core STATIC ${SRC_FILES})

target_compile_definitions(
    typesense-server PRIVATE
    TYPESENSE_VERSION="${TYPESENSE_VERSION}"
)

target_compile_definitions(
    typesense-core PRIVATE
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

if (APPLE)
    set(STD_LIB -static-libstdc++) # https://stackoverflow.com/a/26543140/131050 (can't statically link libgcc on Mac)

    FIND_PACKAGE(ngHTTP2 REQUIRED)
    set(CURL_LIBRARIES ${NGHTTP2_LIBRARIES} ${CURL_LIBRARIES}) # nghttp2 was not being statically built
else()
    set(STD_LIB -static-libgcc -static-libstdc++)
endif()

set(ROCKSDB_LIBS rocksdb ${SNAPPY_LIBRARIES} ${ZLIB_LIBRARIES})

if(NOT APPLE)
    list(APPEND ROCKSDB_LIBS rt)
endif()

set(ICU_ALL_LIBRARIES ${ICU_I18N_LIBRARIES} ${ICU_LIBRARIES} ${ICU_DATA_LIBRARIES})
set(CORE_LIBS h2o-evloop iconv ${CURL_LIBRARIES} for ${ICU_ALL_LIBRARIES} ${G3LOGGER_LIBRARIES}
              ${ROCKSDB_LIBS} ${OPENSSL_LIBRARIES} pthread dl ${STD_LIB})

target_link_libraries(typesense-core ${CORE_LIBS})
target_link_libraries(typesense-server ${CORE_LIBS})
target_link_libraries(search ${CORE_LIBS})
target_link_libraries(benchmark ${CORE_LIBS})
target_link_libraries(typesense-test ${CORE_LIBS} gtest gtest_main)
