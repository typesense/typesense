cmake_minimum_required(VERSION 2.8)
project(typesense)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0")
# set(CMAKE_EXE_LINKER_FLAGS "--verbose")
set(DEP_ROOT_DIR ${CMAKE_SOURCE_DIR}/external-${CMAKE_SYSTEM_NAME})

if(NOT EXISTS ${DEP_ROOT_DIR})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR})
endif()

include(cmake/For.cmake)
include(cmake/H2O.cmake)
include(cmake/RocksDB.cmake)
include(cmake/GoogleTest.cmake)
include(cmake/TestResources.cmake)

if (APPLE)
    add_definitions(-DGTEST_USE_OWN_TR1_TUPLE)
    add_definitions(-D__GLIBCXX__)

    # For openssl installed with Brew
    include_directories(/usr/local/opt/openssl/include)
    link_directories(/usr/local/opt/openssl/lib)
endif (APPLE)

FILE(GLOB HEADER_FILES include/*)
FILE(GLOB SRC_FILES src/*.cpp)

include_directories(include)
include_directories(${DEP_ROOT_DIR}/${FOR_NAME})
include_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/include)
include_directories(${DEP_ROOT_DIR}/${H2O_NAME}/include)
include_directories(${DEP_ROOT_DIR}/${H2O_NAME}/build/libressl-build/include)
include_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME}/include)

link_directories(${DEP_ROOT_DIR}/${GTEST_NAME}/googletest/build)
link_directories(${DEP_ROOT_DIR}/${FOR_NAME})
link_directories(${DEP_ROOT_DIR}/${H2O_NAME}/build)
link_directories(${DEP_ROOT_DIR}/${ROCKSDB_NAME})

add_executable(typesense-server ${SRC_FILES} src/main/server.cpp)
add_executable(search ${SRC_FILES} src/main/main.cpp)
add_executable(benchmark ${SRC_FILES} src/main/benchmark.cpp)
add_executable(typesense_test test/array_test.cpp test/sorted_array_test.cpp test/art_test.cpp
               test/collection_test.cpp test/collection_manager_test.cpp test/topster_test.cpp ${HEADER_FILES} ${SRC_FILES})

target_compile_definitions(typesense-server PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(search PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(benchmark PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(typesense_test PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")

target_link_libraries(typesense-server for h2o-evloop pthread rocksdb ssl crypto dl)
target_link_libraries(search for pthread rocksdb)
target_link_libraries(benchmark for pthread rocksdb)
target_link_libraries(typesense_test pthread for rocksdb gtest gtest_main)
