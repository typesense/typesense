set(BRAFT_VERSION fb27e63)
set(BRAFT_NAME braft-${BRAFT_VERSION})
set(BRAFT_TAR_PATH ${DEP_ROOT_DIR}/${BRAFT_NAME}.tar.gz)

if(NOT EXISTS ${BRAFT_TAR_PATH})
    message(STATUS "Downloading braft...")
    file(DOWNLOAD https://dl.typesense.org/deps/${BRAFT_NAME}-${CMAKE_SYSTEM_NAME}.tar.gz ${BRAFT_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${BRAFT_NAME})
    message(STATUS "Extracting braft...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${BRAFT_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR})
endif()
