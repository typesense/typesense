cmake_minimum_required(VERSION 2.8)
project(miniutf)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -std=c++11 -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -std=c++11 -O0")

FILE(GLOB SRC_FILES miniutf.cpp)

add_library(miniutf STATIC ${SRC_FILES})
target_link_libraries(miniutf)