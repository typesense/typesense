# Download and build libiconv

set(ICONV_VERSION 1.15)
set(ICONV_NAME libiconv-${ICONV_VERSION})
set(ICONV_TAR_PATH ${DEP_ROOT_DIR}/${ICONV_NAME}.tar.gz)

if(NOT EXISTS ${ICONV_TAR_PATH})
    message(STATUS "Downloading libconv...")
    file(DOWNLOAD https://ftp.gnu.org/pub/gnu/libiconv/libiconv-${ICONV_VERSION}.tar.gz ${ICONV_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${ICONV_NAME})
    message(STATUS "Extracting libconv...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${ICONV_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${ICONV_NAME}/Makefile AND BUILD_DEPS STREQUAL "yes")
    message("Configuring libconv locally...")
    execute_process(COMMAND ./configure "--enable-static=yes" "--enable-shared=no" WORKING_DIRECTORY ${DEP_ROOT_DIR}/${ICONV_NAME}/ RESULT_VARIABLE ICONV_CONFIGURE)
    if(NOT ICONV_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "${ICONV_NAME} configure failed!")
    endif()
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs/libiconv.a AND BUILD_DEPS STREQUAL "yes")
    message("Building libconv locally...")
    execute_process(COMMAND make WORKING_DIRECTORY ${DEP_ROOT_DIR}/${ICONV_NAME})
    if(NOT EXISTS ${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs/libiconv.a)
        message(FATAL_ERROR "${ICONV_NAME} build failed!")
    endif()
endif()
