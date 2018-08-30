# Download and build g3log

set(G3LOG_VERSION 1.3.2)
set(G3LOG_NAME g3log-${G3LOG_VERSION})
set(G3LOG_TAR_PATH ${DEP_ROOT_DIR}/${G3LOG_NAME}.tar.gz)

# Specifically pick the static version since the build generates both static and dynamic libraries
set(G3LOGGER_LIBRARIES "${DEP_ROOT_DIR}/${G3LOG_NAME}/build/libg3logger.a")

if(NOT EXISTS ${G3LOG_TAR_PATH})
    message(STATUS "Downloading G3log...")
    file(DOWNLOAD https://github.com/KjellKod/g3log/archive/${G3LOG_VERSION}.tar.gz ${G3LOG_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${G3LOG_NAME})
    message(STATUS "Extracting G3log...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${G3LOG_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${G3LOG_NAME}/build)
    message("Configuring G3log...")
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${G3LOG_NAME}/build)
    execute_process(COMMAND ${CMAKE_COMMAND}
            "-DCMAKE_BUILD_TYPE=Release"
            "-DG3_SHARED_LIB=OFF"
            "-H${DEP_ROOT_DIR}/${G3LOG_NAME}"
            "-B${DEP_ROOT_DIR}/${G3LOG_NAME}/build"
            RESULT_VARIABLE
            G3LOG_CONFIGURE)
    if(NOT G3LOG_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "G3log Configure failed!")
    endif()

    if(BUILD_DEPS STREQUAL "yes")
        message("Building G3log locally...")
        execute_process(COMMAND ${CMAKE_COMMAND} --build
                "${DEP_ROOT_DIR}/${G3LOG_NAME}/build"
                RESULT_VARIABLE
                G3LOG_BUILD)
        if(NOT G3LOG_BUILD EQUAL 0)
            message(FATAL_ERROR "G3log build failed!")
        endif()
    endif()
endif()
