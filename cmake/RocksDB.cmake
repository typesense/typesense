# Download and build RocksDB

set(ROCKSDB_VERSION 4.11.2)
set(ROCKSDB_NAME rocksdb-${ROCKSDB_VERSION})
set(ROCKSDB_TAR_PATH ${DEP_ROOT_DIR}/${ROCKSDB_NAME}.tar.gz)

if(NOT EXISTS ${ROCKSDB_TAR_PATH})
    message(STATUS "Downloading ${ROCKSDB_NAME}...")
    file(DOWNLOAD https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz ${ROCKSDB_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${ROCKSDB_NAME})
    message(STATUS "Extracting ${ROCKSDB_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xvzf ${ROCKSDB_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/librocksdb.so)
    message("Building ${ROCKSDB_NAME} locally...")
    execute_process(COMMAND make "shared_lib" WORKING_DIRECTORY ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/)
endif()