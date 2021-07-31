# Download and build RocksDB

set(ROCKSDB_VERSION 6.20.3)
set(ROCKSDB_NAME rocksdb-${ROCKSDB_VERSION})
set(ROCKSDB_TAR_PATH ${DEP_ROOT_DIR}/${ROCKSDB_NAME}.tar.gz)

if(NOT EXISTS ${ROCKSDB_TAR_PATH})
    message(STATUS "Downloading ${ROCKSDB_NAME}...")
    file(DOWNLOAD https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz ${ROCKSDB_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${ROCKSDB_NAME})
    message(STATUS "Extracting ${ROCKSDB_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${ROCKSDB_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/librocksdb.a AND BUILD_DEPS STREQUAL "yes")
    message("Building ${ROCKSDB_NAME} locally...")
    set(ENV{PORTABLE} 1)
    set(ENV{DEBUG_LEVEL} 0)
    set(ENV{USE_RTTI} 1)
    set(ENV{PLATFORM_LDFLAGS} "-Wl,-Bstatic")

    set(ENV{ROCKSDB_DISABLE_GFLAGS} 1)
    set(ENV{ROCKSDB_DISABLE_BZIP} 1)
    set(ENV{ROCKSDB_DISABLE_LZ4} 1)
    set(ENV{ROCKSDB_DISABLE_ZSTD} 1)
    set(ENV{ROCKSDB_DISABLE_NUMA} 1)
    set(ENV{ROCKSDB_DISABLE_TBB} 1)
    set(ENV{ROCKSDB_DISABLE_JEMALLOC} 1)
    set(ENV{ROCKSDB_DISABLE_TCMALLOC} 1)
    set(ENV{ROCKSDB_DISABLE_BACKTRACE} 1)
    set(ENV{ROCKSDB_DISABLE_PG} 1)

    message(STATUS "Cleaning...")
    execute_process(COMMAND make "clean" WORKING_DIRECTORY ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/)

    message(STATUS "Building static library...")
    execute_process(COMMAND make "static_lib" WORKING_DIRECTORY ${DEP_ROOT_DIR}/${ROCKSDB_NAME}/
            RESULT_VARIABLE ROCKSDB_BUILD)
    if(NOT ROCKSDB_BUILD EQUAL 0)
        message(FATAL_ERROR "${ROCKSDB_NAME} build failed!")
    endif()
endif()
