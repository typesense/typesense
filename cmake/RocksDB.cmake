# Download and build RocksDB

set(ROCKSDB_VERSION 4.13.5)
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

# Use patched build detect platform script to control what libraries RocksDB links against
file(COPY ${CMAKE_SOURCE_DIR}/cmake/patches/build_detect_platform DESTINATION
        ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/build_tools)

if(NOT EXISTS ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/librocksdb.a)
    message("Building ${ROCKSDB_NAME} locally...")
    execute_process(COMMAND make "static_lib" WORKING_DIRECTORY ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/
                    RESULT_VARIABLE ROCKSDB_BUILD)
    if(NOT ROCKSDB_BUILD EQUAL 0)
        message(FATAL_ERROR "${ROCKSDB_NAME} build failed!")
    endif()
endif()