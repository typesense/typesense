# Download and build libfor

set(KAKASI_VERSION 9e0825a02c7ea5605e968f6208f769f7c49d6860)
set(KAKASI_NAME kakasi-${KAKASI_VERSION})
set(KAKASI_TAR_PATH ${DEP_ROOT_DIR}/${KAKASI_NAME}.tar.gz)

if(NOT EXISTS ${KAKASI_TAR_PATH})
    message(STATUS "Downloading kakasi...")
    file(DOWNLOAD https://github.com/typesense/kakasi/archive/${KAKASI_VERSION}.tar.gz ${KAKASI_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${KAKASI_NAME})
    message(STATUS "Extracting kakasi...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${KAKASI_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${KAKASI_NAME}/build/lib/libkakasi.a AND BUILD_DEPS STREQUAL "yes")
    set(ENV{LIBRARY_PATH} "${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs")
    set(ENV{CFLAGS} "-I${DEP_ROOT_DIR}/${ICONV_NAME}/include -g")
    set(ENV{LDFLAGS} "-g")

    message("Configuring kakasi...")
    execute_process(COMMAND ./configure
            "--prefix=${DEP_ROOT_DIR}/${KAKASI_NAME}/build"
            "--enable-shared=no"
            WORKING_DIRECTORY ${DEP_ROOT_DIR}/${KAKASI_NAME}/ RESULT_VARIABLE KAKASI_CONFIGURE)
    if(NOT KAKASI_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "${KAKASI_NAME} configure failed!")
    endif()

    message("Building kakasi locally...")

    execute_process(COMMAND make WORKING_DIRECTORY ${DEP_ROOT_DIR}/${KAKASI_NAME}/
                    RESULT_VARIABLE KAKASI_BUILD)
    if(NOT KAKASI_BUILD EQUAL 0)
        message(FATAL_ERROR "${KAKASI_NAME} build failed!")
    endif()

    message("Installing kakasi locally...")
    execute_process(COMMAND make install WORKING_DIRECTORY ${DEP_ROOT_DIR}/${KAKASI_NAME}/
            RESULT_VARIABLE KAKASI_INSTALL)
    if(NOT KAKASI_INSTALL EQUAL 0)
        message(FATAL_ERROR "${KAKASI_NAME} install failed!")
    endif()
endif()
