cmake_minimum_required(VERSION 2.8)
project(typesense)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0")

# Download and compile gtest -------------------------------------------------------------------------------------------
if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0)
    set(GTEST_TAR_PATH ${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0.tar.gz)
    message(STATUS "Downloading and extracting Google Test...")
    file(DOWNLOAD https://github.com/google/googletest/archive/release-1.8.0.tar.gz ${GTEST_TAR_PATH})
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xvzf ${GTEST_TAR_PATH})
    file(RENAME ${CMAKE_SOURCE_DIR}/googletest-release-1.8.0 ${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0)
endif()

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0/googletest/build)
    message("Configuring Google Test...")
    file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0/googletest/build)
    execute_process(COMMAND ${CMAKE_COMMAND}
            "-H${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0/googletest"
            "-B${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0/googletest/build"
            RESULT_VARIABLE
            GOOGLETEST_CONFIGURE)
    if(NOT GOOGLETEST_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "Google Test Configure failed!")
    endif()

    message("Building Google Test locally...")
    execute_process(COMMAND ${CMAKE_COMMAND} --build
            "${CMAKE_SOURCE_DIR}/external/googletest-release-1.8.0/googletest/build"
            RESULT_VARIABLE
            GOOGLETEST_BUILD)
    if(NOT GOOGLETEST_BUILD EQUAL 0)
        message(FATAL_ERROR "Google Test build failed!")
    endif()
endif()
#-----------------------------------------------------------------------------------------------------------------------

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