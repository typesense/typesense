# Download and build MINIUTF

set(MINIUTF_VERSION 8babf72ddc3e85d794af24520483040fcfbf85f8)
set(MINIUTF_NAME miniutf-${MINIUTF_VERSION})
set(MINIUTF_TAR_PATH ${DEP_ROOT_DIR}/${MINIUTF_NAME}.tar.gz)

if(NOT EXISTS ${MINIUTF_TAR_PATH})
    message(STATUS "Downloading Miniutf...")
    file(DOWNLOAD https://github.com/dropbox/miniutf/archive/${MINIUTF_VERSION}.tar.gz ${MINIUTF_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${MINIUTF_NAME})
    message(STATUS "Extracting Miniutf...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${MINIUTF_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()

# Copy a custom CMakeLists.txt which we will use for the build
file(COPY ${CMAKE_SOURCE_DIR}/cmake/patches/miniutf/CMakeLists.txt DESTINATION
        ${DEP_ROOT_DIR}/${MINIUTF_NAME})

if(NOT EXISTS ${DEP_ROOT_DIR}/${MINIUTF_NAME}/build)
    message("Configuring Miniutf...")
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${MINIUTF_NAME}/build)
    execute_process(COMMAND ${CMAKE_COMMAND}
            "-H${DEP_ROOT_DIR}/${MINIUTF_NAME}"
            "-B${DEP_ROOT_DIR}/${MINIUTF_NAME}/build"
            RESULT_VARIABLE
            MINIUTF_CONFIGURE)
    if(NOT MINIUTF_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "Miniutf Configure failed!")
    endif()

    message("Building Miniutf locally...")
    execute_process(COMMAND ${CMAKE_COMMAND} --build
            "${DEP_ROOT_DIR}/${MINIUTF_NAME}/build"
            RESULT_VARIABLE
            MINIUTF_BUILD)
    if(NOT MINIUTF_BUILD EQUAL 0)
        message(FATAL_ERROR "Miniutf build failed!")
    endif()
endif()