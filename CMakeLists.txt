cmake_minimum_required(VERSION 2.8)
project(search)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories(include)
#file(GLOB SOURCES "src/*.cpp")

add_executable(search src/art.c src/main.cpp)