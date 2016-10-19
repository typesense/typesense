# Download and build gtest

set(GTEST_VERSION 1.8.0)
set(GTEST_NAME googletest-release-${GTEST_VERSION})
set(GTEST_TAR_PATH ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME}.tar.gz)

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME})
    message(STATUS "Downloading and extracting Google Test...")
    file(DOWNLOAD https://github.com/google/googletest/archive/release-1.8.0.tar.gz ${GTEST_TAR_PATH})
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xvzf ${GTEST_TAR_PATH} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external/)
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