cmake_minimum_required(VERSION 2.8)
project(search)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++ -std=gnu++11 -O2")

include_directories(include)
include_directories(external/EWAHBoolArray/headers)

# Setup roaring as a static lib ---
include_directories(external/croaring/include external/croaring/include/containers external/croaring/include/misc)

add_library(array STATIC external/croaring/array.o)
SET_SOURCE_FILES_PROPERTIES(external/croaring/array.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
SET_TARGET_PROPERTIES(array PROPERTIES LINKER_LANGUAGE C)

add_library(roaring_array STATIC external/croaring/roaring_array.o)
SET_SOURCE_FILES_PROPERTIES(external/croaring/roaring_array.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
SET_TARGET_PROPERTIES(roaring_array PROPERTIES LINKER_LANGUAGE C)

add_library(bitset STATIC external/croaring/bitset.o)
SET_SOURCE_FILES_PROPERTIES(external/croaring/bitset.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
SET_TARGET_PROPERTIES(bitset PROPERTIES LINKER_LANGUAGE C)

add_library(containers STATIC external/croaring/containers.o)
SET_SOURCE_FILES_PROPERTIES(external/croaring/containers.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
SET_TARGET_PROPERTIES(containers PROPERTIES LINKER_LANGUAGE C)

add_library(run STATIC external/croaring/run.o)
SET_SOURCE_FILES_PROPERTIES(external/croaring/run.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
SET_TARGET_PROPERTIES(run PROPERTIES LINKER_LANGUAGE C)

add_library(util STATIC external/croaring/util.o)
SET_SOURCE_FILES_PROPERTIES(external/croaring/util.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
SET_TARGET_PROPERTIES(util PROPERTIES LINKER_LANGUAGE C)

add_library(roaring STATIC external/croaring/roaring.o)
SET_SOURCE_FILES_PROPERTIES(external/croaring/roaring.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
SET_TARGET_PROPERTIES(roaring PROPERTIES LINKER_LANGUAGE C)

# End Roaring as static lib ---

add_executable(search src/art.cpp src/main.cpp)
target_link_libraries(search roaring roaring_array array bitset containers run util for)
