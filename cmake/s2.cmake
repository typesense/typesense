# Download and build S2

set(S2_VERSION efb124d8eaf3433323d3e877dedd5e94a63339a3)
set(S2_NAME s2geometry-${S2_VERSION})
set(S2_TAR_PATH ${DEP_ROOT_DIR}/${S2_NAME}.tar.gz)

if(NOT EXISTS ${S2_TAR_PATH})
    message(STATUS "Downloading https://github.com/google/s2geometry/archive/${S2_VERSION}.tar.gz")
    file(DOWNLOAD https://github.com/google/s2geometry/archive/${S2_VERSION}.tar.gz ${S2_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${S2_NAME})
    message(STATUS "Extracting ${S2_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${S2_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${S2_NAME}/build/libs2.a)
    message("Configuring ${S2_NAME}...")

    file(REMOVE_RECURSE ${DEP_ROOT_DIR}/${S2_NAME}/build)
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${S2_NAME}/build)

    execute_process(COMMAND ${CMAKE_COMMAND}
            "-DCMAKE_FIND_LIBRARY_SUFFIXES=.a"
            "-DBUILD_SHARED_LIBS=OFF"
            "-H${DEP_ROOT_DIR}/${S2_NAME}"
            "-B${DEP_ROOT_DIR}/${S2_NAME}/build"
            RESULT_VARIABLE
            S2_CONFIGURE)
    if(NOT S2_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "${S2_NAME} configure failed!")
    endif()

    if(BUILD_DEPS STREQUAL "yes")
        message("Building ${S2_NAME} locally...")
        execute_process(COMMAND ${CMAKE_COMMAND} --build
                "${DEP_ROOT_DIR}/${S2_NAME}/build"
                --target s2
                RESULT_VARIABLE
                S2_BUILD)
        if(NOT S2_BUILD EQUAL 0)
            message(FATAL_ERROR "${S2_NAME} build failed!")
        endif()
    endif()
endif()
