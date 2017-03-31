# Download and build H2O

set(H2O_VERSION 2.0.4)
set(H2O_NAME h2o-${H2O_VERSION})
set(H2O_TAR_PATH ${DEP_ROOT_DIR}/${H2O_NAME}.tar.gz)

if(NOT EXISTS ${H2O_TAR_PATH})
    message(STATUS "Downloading ${H2O_NAME}...")
    file(DOWNLOAD https://github.com/h2o/h2o/archive/v${H2O_VERSION}.tar.gz ${H2O_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${H2O_NAME})
    message(STATUS "Extracting ${H2O_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xvzf ${H2O_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${H2O_NAME}/build/h2o)
    message("Configuring ${H2O_NAME}...")
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${H2O_NAME}/build)
    execute_process(COMMAND ${CMAKE_COMMAND}
            "-DWITH_BUNDLED_SSL=off"
            "-DWITH_MRUBY=off"
            "-H${DEP_ROOT_DIR}/${H2O_NAME}"
            "-B${DEP_ROOT_DIR}/${H2O_NAME}/build"
            RESULT_VARIABLE
            H2O_CONFIGURE)
    if(NOT H2O_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "${H2O_NAME} configure failed!")
    endif()

    message("Building ${H2O_NAME} locally...")
    execute_process(COMMAND ${CMAKE_COMMAND} --build
            "${DEP_ROOT_DIR}/${H2O_NAME}/build"
            RESULT_VARIABLE
            H2O_BUILD)
    if(NOT H2O_BUILD EQUAL 0)
        message(FATAL_ERROR "${H2O_NAME} build failed!")
    endif()
endif()