# Download and build H3

set(H3_VERSION 3.7.1)
set(H3_NAME h3-${H3_VERSION})
set(H3_TAR_PATH ${DEP_ROOT_DIR}/${H3_NAME}.tar.gz)

if(NOT EXISTS ${H3_TAR_PATH})
    message(STATUS "Downloading https://github.com/uber/h3/archive/v${H3_VERSION}.tar.gz")
    file(DOWNLOAD https://github.com/uber/h3/archive/v${H3_VERSION}.tar.gz ${H3_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${H3_NAME})
    message(STATUS "Extracting ${H3_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${H3_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${H3_NAME}/build/lib/libh3.a)
    message("Configuring ${H3_NAME}...")
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${H3_NAME}/build)

    execute_process(COMMAND ${CMAKE_COMMAND}
            "-DCMAKE_FIND_LIBRARY_SUFFIXES=.a"
            "-H${DEP_ROOT_DIR}/${H3_NAME}"
            "-B${DEP_ROOT_DIR}/${H3_NAME}/build"
            RESULT_VARIABLE
            H3_CONFIGURE)
    if(NOT H3_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "${H3_NAME} configure failed!")
    endif()

    if(BUILD_DEPS STREQUAL "yes")
        message("Building ${H3_NAME} locally...")
        execute_process(COMMAND ${CMAKE_COMMAND} --build
                "${DEP_ROOT_DIR}/${H3_NAME}/build"
                RESULT_VARIABLE
                H3_BUILD)
        if(NOT H3_BUILD EQUAL 0)
            message(FATAL_ERROR "${H3_NAME} build failed!")
        endif()
    endif()
endif()
