# Download and build LRUCACHE

set(LRUCACHE_VERSION 13f30ad33a227a3e9682578c450777380ecddfcf)
set(LRUCACHE_NAME lru-cache-${LRUCACHE_VERSION})
set(LRUCACHE_TAR_PATH ${DEP_ROOT_DIR}/${LRUCACHE_NAME}.tar.gz)

if(NOT EXISTS ${LRUCACHE_TAR_PATH})
    message(STATUS "Downloading https://github.com/goldsborough/lru-cache/archive/${LRUCACHE_VERSION}.tar.gz")
    file(DOWNLOAD https://github.com/goldsborough/lru-cache/archive/${LRUCACHE_VERSION}.tar.gz ${LRUCACHE_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${LRUCACHE_NAME})
    message(STATUS "Extracting ${LRUCACHE_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${LRUCACHE_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()
