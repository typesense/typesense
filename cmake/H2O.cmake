# Download and build gtest

set(H2O_VERSION 2.0.4)
set(H2O_NAME h2o-${H2O_VERSION})
set(H2O_TAR_PATH ${CMAKE_SOURCE_DIR}/external/${H2O_NAME}.tar.gz)

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/${H2O_NAME})
    message(STATUS "Downloading and extracting ${H2O_NAME}...")
    file(DOWNLOAD https://github.com/h2o/h2o/archive/v2.0.4.tar.gz ${H2O_TAR_PATH})
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xvzf ${H2O_TAR_PATH} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external/)
endif()

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/build)
    message("Configuring ${H2O_NAME}...")
    file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/build)
    execute_process(COMMAND ${CMAKE_COMMAND}
            "-DWITH_BUNDLED_SSL=on"
            "-H${CMAKE_SOURCE_DIR}/external/${H2O_NAME}"
            "-B${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/build"
            RESULT_VARIABLE
            H2O_CONFIGURE)
    if(NOT H2O_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "${H2O_NAME} configure failed!")
    endif()

    message("Building ${H2O_NAME} locally...")
    execute_process(COMMAND ${CMAKE_COMMAND} --build
            "${CMAKE_SOURCE_DIR}/external/${H2O_NAME}/build"
            RESULT_VARIABLE
            H2O_BUILD)
    if(NOT H2O_BUILD EQUAL 0)
        message(FATAL_ERROR "${H2O_NAME} build failed!")
    endif()
endif()