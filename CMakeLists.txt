cmake_minimum_required(VERSION 2.8)
project(typesense)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0")

include(GoogleTest.cmake)

if (APPLE)
    add_definitions(-DGTEST_USE_OWN_TR1_TUPLE)
    add_definitions(-D__GLIBCXX__)
    include_directories(/usr/local/opt/openssl/include) # For openssl installed with brew
endif (APPLE)

# FIXME: Need to use find_package() to locate the correct openssl library
# FIXME: build libfor as part of the build process

FILE(GLOB HEADER_FILES include/*)

include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/external/libfor)
include_directories(${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0/googletest/include)

link_directories(${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0/googletest/build)

add_executable(typesense_test test/forarray_test.cpp)
add_executable(search ${HEADER_FILES} src/art.cpp src/intersection.cpp src/main.cpp src/collection.cpp)
add_executable(typesense-server ${HEADER_FILES} src/art.cpp src/intersection.cpp src/collection.cpp src/server.cpp)

target_link_libraries(typesense_test
    ${CMAKE_SOURCE_DIR}/external/libfor/libfor.a
    gtest gtest_main
)

target_link_libraries(search ${CMAKE_SOURCE_DIR}/external/libfor/libfor.a rocksdb)
target_link_libraries(typesense-server
        ${CMAKE_SOURCE_DIR}/external/libfor/libfor.a
        curl h2o-evloop pthread rocksdb
        /usr/local/opt/openssl/lib/libssl.a
        /usr/local/opt/openssl/lib/libcrypto.a)