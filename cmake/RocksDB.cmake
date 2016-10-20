# Download and build RocksDB

set(ROCKSDB_VERSION 4.11.2)
set(ROCKSDB_NAME rocksdb-${ROCKSDB_VERSION})
set(ROCKSDB_TAR_PATH ${CMAKE_SOURCE_DIR}/external/${ROCKSDB_NAME}.tar.gz)

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/${ROCKSDB_NAME})
    message(STATUS "Downloading and extracting ${ROCKSDB_NAME}...")
    file(DOWNLOAD https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz ${ROCKSDB_TAR_PATH})
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xvzf ${ROCKSDB_TAR_PATH} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external/)
endif()

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/external/${ROCKSDB_NAME}/libfor.a)
    message("Building ${ROCKSDB_NAME} locally...")
    execute_process(COMMAND make "static_lib" WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${ROCKSDB_NAME}/)
endif()