# Download and build libfor

set(FOR_VERSION 49611808d08d4e47116aa2a3ddcabeb418f405f7)
set(FOR_NAME libfor-${FOR_VERSION})
set(FOR_TAR_PATH ${CMAKE_SOURCE_DIR}/external/${FOR_NAME}.tar.gz)

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/${FOR_NAME})
    message(STATUS "Downloading and extracting libfor...")
    file(DOWNLOAD https://github.com/cruppstahl/libfor/archive/${FOR_VERSION}.tar.gz ${FOR_TAR_PATH})
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xvzf ${FOR_TAR_PATH} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external/)
endif()

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/${FOR_NAME}/libfor.a)
    message("Building libfor locally...")
    execute_process(COMMAND make WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${FOR_NAME}/)
endif()