# Download and build EASYLOGGINGPP

set(EASYLOGGINGPP_VERSION 9.95.3)
set(EASYLOGGINGPP_NAME easyloggingpp-${EASYLOGGINGPP_VERSION})
set(EASYLOGGINGPP_TAR_PATH ${DEP_ROOT_DIR}/${EASYLOGGINGPP_NAME}.tar.gz)

if(NOT EXISTS ${EASYLOGGINGPP_TAR_PATH})
    message(STATUS "Downloading Easyloggingpp...")
    file(DOWNLOAD https://github.com/muflihun/easyloggingpp/archive/v${EASYLOGGINGPP_VERSION}.tar.gz ${EASYLOGGINGPP_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${EASYLOGGINGPP_NAME})
    message(STATUS "Extracting Easyloggingpp...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${EASYLOGGINGPP_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${EASYLOGGINGPP_NAME}/build)
    message("Configuring Easyloggingpp...")
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${EASYLOGGINGPP_NAME}/build)
    execute_process(COMMAND ${CMAKE_COMMAND}
            "-Dbuild_static_lib=ON"
            "-H${DEP_ROOT_DIR}/${EASYLOGGINGPP_NAME}"
            "-B${DEP_ROOT_DIR}/${EASYLOGGINGPP_NAME}/build"
            RESULT_VARIABLE
            EASYLOGGINGPP_CONFIGURE)
    if(NOT EASYLOGGINGPP_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "Easyloggingpp Configure failed!")
    endif()

    message("Building Easyloggingpp locally...")
    execute_process(COMMAND ${CMAKE_COMMAND} --build
            "${DEP_ROOT_DIR}/${EASYLOGGINGPP_NAME}/build"
            RESULT_VARIABLE
            EASYLOGGINGPP_BUILD)
    if(NOT EASYLOGGINGPP_BUILD EQUAL 0)
        message(FATAL_ERROR "Easyloggingpp build failed!")
    endif()
endif()