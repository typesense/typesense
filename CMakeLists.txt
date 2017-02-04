cmake_minimum_required(VERSION 2.8)
project(typesense)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0")

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/)
    file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/external)
endif()

include(cmake/For.cmake)
include(cmake/H2O.cmake)
include(cmake/RocksDB.cmake)
include(cmake/GoogleTest.cmake)
include(cmake/TestResources.cmake)

if (APPLE)
    add_definitions(-DGTEST_USE_OWN_TR1_TUPLE)
    add_definitions(-D__GLIBCXX__)
endif (APPLE)

FILE(GLOB HEADER_FILES include/*)
FILE(GLOB SRC_FILES src/*.cpp)

include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/external/${FOR_NAME})
include_directories(${CMAKE_SOURCE_DIR}/external/${GTEST_NAME}/googletest/include)
include_directories(${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/include)
include_directories(${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/build/libressl-build/include)
include_directories(${CMAKE_SOURCE_DIR}/external/${ROCKSDB_NAME}/include)

link_directories(${CMAKE_SOURCE_DIR}/external/${GTEST_NAME}/googletest/build)
link_directories(${CMAKE_SOURCE_DIR}/external/${FOR_NAME})
link_directories(${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/build)
link_directories(${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/build/libressl-build/lib)
link_directories(${CMAKE_SOURCE_DIR}/external/${ROCKSDB_NAME})

add_executable(typesense-server ${HEADER_FILES} ${SRC_FILES} src/main/server.cpp)
add_executable(search ${HEADER_FILES} ${SRC_FILES} src/main/main.cpp)
add_executable(benchmark ${HEADER_FILES} ${SRC_FILES} src/main/benchmark.cpp)
add_executable(typesense_test test/array_test.cpp test/sorted_array_test.cpp test/art_test.cpp
               test/collection_test.cpp test/collection_manager_test.cpp test/topster_test.cpp ${SRC_FILES})

target_compile_definitions(typesense-server PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(search PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(benchmark PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")
target_compile_definitions(typesense_test PRIVATE ROOT_DIR="${CMAKE_SOURCE_DIR}/")

target_link_libraries(typesense-server for curl h2o-evloop pthread rocksdb ssl crypto)
target_link_libraries(search for pthread rocksdb)
target_link_libraries(benchmark for pthread rocksdb)
target_link_libraries(typesense_test pthread for rocksdb gtest gtest_main)
