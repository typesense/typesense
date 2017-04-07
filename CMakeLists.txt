cmake_minimum_required(VERSION 2.8)
project(typesense)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0")
# set(CMAKE_EXE_LINKER_FLAGS "--verbose")
set(DEP_ROOT_DIR ${CMAKE_SOURCE_DIR}/external-${CMAKE_SYSTEM_NAME})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

if(NOT EXISTS ${DEP_ROOT_DIR})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR})
endif()

if (APPLE)
    add_definitions(-DGTEST_USE_OWN_TR1_TUPLE)
    add_definitions(-D__GLIBCXX__)

    # For Brew
    set(ENV{OPENSSL_ROOT_DIR} /usr/local/opt/openssl)
    set(SNAPPY_ROOT_DIR /usr/local/opt/snappy)
    set(ZLIB_ROOT /usr/local/opt/zlib)
endif (APPLE)

# Ensure that we only use the static versions of libraries to link against
# You can't statically link a shared library (or dynamically link a static library)
IF(WIN32 OR MSVC)
    SET(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a)
ELSE()
    SET(CMAKE_FIND_LIBRARY_SUFFIXES .a)
ENDIF()

function(find_static_library LIB_NAME OUT)
    find_library(FOUND_${LIB_NAME} ${LIB_NAME})
    message("FINDING: ${LIB_NAME} and got ${FOUND_${LIB_NAME}}")

    if(NOT FOUND_${LIB_NAME})
        message(FATAL_ERROR "Unable to find static library ${LIB_NAME}")
    endif()

    set(${OUT} ${FOUND_${LIB_NAME}} PARENT_SCOPE)
endfunction()

FIND_PACKAGE(OpenSSL 1.0.2 REQUIRED)
FIND_PACKAGE(Snappy REQUIRED)
FIND_PACKAGE(ZLIB REQUIRED)

include(cmake/For.cmake)
include(cmake/H2O.cmake)
include(cmake/RocksDB.cmake)
include(cmake/GoogleTest.cmake)
include(cmake/TestResources.cmake)

FILE(GLOB HEADER_FILES include/*)
FILE(GLOB SRC_FILES src/*.cpp)

include_directories(include)
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${DEP_ROOT_DIR}/${FOR_NAME})
include_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/include)
include_directories(${DEP_ROOT_DIR}/${H2O_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME}/include)

link_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/build)
link_directories(${DEP_ROOT_DIR}/${FOR_NAME})
link_directories(${DEP_ROOT_DIR}/${H2O_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME})

add_executable(typesense-server ${SRC_FILES} src/main/server.cpp)
add_executable(search ${SRC_FILES} src/main/main.cpp)
add_executable(benchmark ${SRC_FILES} src/main/benchmark.cpp)
add_executable(typesense_test ${SRC_FILES} test/array_test.cpp test/sorted_array_test.cpp test/art_test.cpp
               test/collection_test.cpp test/collection_manager_test.cpp test/topster_test.cpp)

target_compile_definitions(typesense-server PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(search PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(benchmark PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(typesense_test PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")

target_link_libraries(typesense-server for h2o-evloop pthread rocksdb ${SNAPPY_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} dl)
target_link_libraries(search for pthread rocksdb ${SNAPPY_LIBRARIES} ${ZLIB_LIBRARIES})
target_link_libraries(benchmark for pthread rocksdb ${SNAPPY_LIBRARIES} ${ZLIB_LIBRARIES})
target_link_libraries(typesense_test pthread for rocksdb ${SNAPPY_LIBRARIES} ${ZLIB_LIBRARIES} gtest gtest_main)
