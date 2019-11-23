# Download and build H2O

set(MDB_CONN_VERSION 3.1.5)
set(MDB_CONN_NAME mariadb-connector-c-${MDB_CONN_VERSION})
set(MDB_CONN_TAR_PATH ${DEP_ROOT_DIR}/${MDB_CONN_NAME}.tar.gz)

if(NOT EXISTS ${MDB_CONN_TAR_PATH})
    message(STATUS "Downloading ${MDB_CONN_NAME}...")
    file(DOWNLOAD https://github.com/MariaDB/mariadb-connector-c/archive/v${MDB_CONN_VERSION}.tar.gz ${MDB_CONN_TAR_PATH})
endif()

if(NOT EXISTS ${DEP_ROOT_DIR}/${MDB_CONN_NAME})
    message(STATUS "Extracting ${MDB_CONN_NAME}...")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf ${MDB_CONN_TAR_PATH} WORKING_DIRECTORY ${DEP_ROOT_DIR}/)
endif()

if (APPLE)
    set(OPENSSL_ROOT_DIR /usr/local/opt/openssl@1.1)
endif (APPLE)

if(NOT EXISTS ${DEP_ROOT_DIR}/${MDB_CONN_NAME}/build/libmariadb/libmariadbclient.a)
    message("Configuring ${MDB_CONN_NAME}...")

    if (APPLE)
        # Must replace this definition since it prevents GNU iconv from working
        # See: https://stackoverflow.com/questions/57734434/libiconv-or-iconv-undefined-symbol-on-mac-osx
        file(READ ${DEP_ROOT_DIR}/${MDB_CONN_NAME}/libmariadb/CMakeLists.txt LIBMARIADB_CMAKE_LISTS)
        string(REPLACE "ADD_DEFINITIONS\(-DLIBICONV_PLUG\)" "" PATCHED_LIBMARIADB_CMAKE_LISTS "${LIBMARIADB_CMAKE_LISTS}")
        file(WRITE ${DEP_ROOT_DIR}/${MDB_CONN_NAME}/libmariadb/CMakeLists.txt "${PATCHED_LIBMARIADB_CMAKE_LISTS}")
    endif (APPLE)

    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${MDB_CONN_NAME}/build)
    execute_process(COMMAND ${CMAKE_COMMAND}
            "-H${DEP_ROOT_DIR}/${MDB_CONN_NAME}"
            "-B${DEP_ROOT_DIR}/${MDB_CONN_NAME}/build"
            "-DCLIENT_PLUGIN_SOCKET=OFF"
            "-DCLIENT_PLUGIN_SHMEM=OFF"
            "-DCLIENT_PLUGIN_NPIPE=OFF"
            "-DCLIENT_PLUGIN_DIALOG=OFF"
            "-DCLIENT_PLUGIN_OLDPASSWORD=OFF"
            "-DCLIENT_PLUGIN_CLEARTEXT=OFF"
            "-DCLIENT_PLUGIN_AUTH_GSSAPI_CLIENT=OFF"
            "-DCLIENT_PLUGIN_SHA256_PASSWORD=OFF"
            "-DCLIENT_PLUGIN_AURORA=OFF"
            "-DCLIENT_PLUGIN_REPLICATION=OFF"
            "-DCMAKE_INCLUDE_PATH=${DEP_ROOT_DIR}/${ICONV_NAME}/include"
            "-DCMAKE_PREFIX_PATH=${DEP_ROOT_DIR}/${ICONV_NAME}/lib/.libs;${OPENSSL_ROOT_DIR}"
            RESULT_VARIABLE
            MDB_CONN_CONFIGURE)
    if(NOT MDB_CONN_CONFIGURE EQUAL 0)
        message(FATAL_ERROR "${MDB_CONN_NAME} configure failed!")
    endif()

    FIND_PACKAGE(OpenSSL 1.1.1 REQUIRED)

    if(BUILD_DEPS STREQUAL "yes")
        message("Building ${MDB_CONN_NAME} locally...")
        execute_process(COMMAND ${CMAKE_COMMAND} --build
                "${DEP_ROOT_DIR}/${MDB_CONN_NAME}/build"
                --config Release
                RESULT_VARIABLE
                MDB_CONN_BUILD)
        if(NOT MDB_CONN_BUILD EQUAL 0)
            message(FATAL_ERROR "${MDB_CONN_NAME} build failed!")
        endif()
    endif()
endif()
