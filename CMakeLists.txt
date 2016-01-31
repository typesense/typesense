cmake_minimum_required(VERSION 2.8)
project(search)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++ -std=gnu++11 -O2")

include_directories(include)

include_directories(external/for)

add_executable(search external/for/libfor.a src/art.cpp src/main.cpp)
target_link_libraries(search for)
