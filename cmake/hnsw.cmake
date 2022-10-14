# Download hnsw (header-only)

set(HNSW_VERSION 7f971d46d6c14c187e6ad8cf237e8e121aae2257)
set(HNSW_NAME hnswlib-${HNSW_VERSION})
set(HNSW_TAR_PATH ${DEP_ROOT_DIR}/${HNSW_NAME}.tar.gz)

if(NOT EXISTS ${HNSW_TAR_PATH})
    message(STATUS "Downloading https://github.com/typesense/hnswlib/archive/${HNSW_VERSION}.tar.gz")
    file(DOWNLOAD https://github.com/typesense/hnswlib/archive/${HNSW_VERSION}.tar.gz ${HNSW_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${HNSW_NAME})
    message(STATUS "Extracting ${HNSW_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${HNSW_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()
