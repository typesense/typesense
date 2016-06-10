cmake_minimum_required(VERSION 2.8)
project(search)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++ -std=gnu++11 -O3")

include_directories(include)
include_directories(external/for)

add_executable(search src/art.cpp src/intersection.cpp src/main.cpp)
target_link_libraries(search ${CMAKE_SOURCE_DIR}/external/for/libfor.a)
