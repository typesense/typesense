# Download and build libfor

set(FOR_VERSION 49611808d08d4e47116aa2a3ddcabeb418f405f7)
set(FOR_NAME libfor-${FOR_VERSION})
set(FOR_TAR_PATH ${DEP_ROOT_DIR}/${FOR_NAME}.tar.gz)

if(NOT EXISTS ${FOR_TAR_PATH})
    message(STATUS "Downloading libfor...")
    file(DOWNLOAD https://github.com/cruppstahl/libfor/archive/${FOR_VERSION}.tar.gz ${FOR_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${FOR_NAME})
    message(STATUS "Extracting libfor...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${FOR_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${FOR_NAME}/libfor.a AND BUILD_DEPS STREQUAL "yes")
    message("Building libfor locally...")
    execute_process(COMMAND make WORKING_DIRECTORY ${DEP_ROOT_DIR}/${FOR_NAME}/
                    RESULT_VARIABLE FOR_BUILD)
    if(NOT FOR_BUILD EQUAL 0)
        message(FATAL_ERROR "${FOR_NAME} build failed!")
    endif()
endif()
