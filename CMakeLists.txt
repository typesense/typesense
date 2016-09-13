cmake_minimum_required(VERSION 2.8)
project(search)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0")

# FIXME: Need to use find_package() to locate the correct openssl library
# FIXME: build libfor as part of the build process

include_directories(include)
include_directories(external/libfor)
include_directories(/usr/local/opt/openssl/include)

add_executable(search src/art.cpp src/intersection.cpp src/main.cpp src/collection.cpp)
add_executable(typesense-server src/art.cpp src/intersection.cpp src/collection.cpp src/server.cpp)

target_link_libraries(search ${CMAKE_SOURCE_DIR}/external/libfor/libfor.a rocksdb)
target_link_libraries(typesense-server ${CMAKE_SOURCE_DIR}/external/libfor/libfor.a curl h2o-evloop pthread /usr/local/opt/openssl/lib/libssl.a /usr/local/opt/openssl/lib/libcrypto.a rocksdb)