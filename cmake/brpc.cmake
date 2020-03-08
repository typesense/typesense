set(BRPC_VERSION 23c66e3)
set(BRPC_NAME brpc-${BRPC_VERSION})
set(BRPC_TAR_PATH ${DEP_ROOT_DIR}/${BRPC_NAME}.tar.gz)

if(NOT EXISTS ${BRPC_TAR_PATH})
    message(STATUS "Downloading brpc...")
    file(DOWNLOAD https://dl.typesense.org/deps/${BRPC_NAME}-${CMAKE_SYSTEM_NAME}.tar.gz ${BRPC_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${BRPC_NAME})
    message(STATUS "Extracting brpc...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${BRPC_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()
