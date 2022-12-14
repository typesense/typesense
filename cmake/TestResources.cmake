# Download test resources

set(ART_VERSION bbbf588bca55bce095538ee8ca8b422904baebc5)
set(TEST_RESOURCES_DIR ${CMAKE_SOURCE_DIR}/build/test_resources)
set(ART_WORDS_PATH ${TEST_RESOURCES_DIR}/words.txt)
set(ART_UUID_PATH ${TEST_RESOURCES_DIR}/uuid.txt)
set(TOKEN_OFFSETS_PATH ${TEST_RESOURCES_DIR}/token_offsets.txt)

if(NOT EXISTS ${TEST_RESOURCES_DIR})
    file(MAKE_DIRECTORY ${TEST_RESOURCES_DIR})
endif()

if(NOT EXISTS ${ART_WORDS_PATH} AND BUILD_DEPS STREQUAL "yes")
    message(STATUS "Downloading test resource - words.txt")
    file(DOWNLOAD https://raw.githubusercontent.com/kishorenc/libart/${ART_VERSION}/tests/words.txt ${ART_WORDS_PATH})
endif()

if(NOT EXISTS ${ART_UUID_PATH} AND BUILD_DEPS STREQUAL "yes")
    message(STATUS "Downloading test resource - uuid.txt")
    file(DOWNLOAD https://raw.githubusercontent.com/kishorenc/libart/${ART_VERSION}/tests/uuid.txt ${ART_UUID_PATH})
endif()

if(NOT EXISTS ${TOKEN_OFFSETS_PATH} AND BUILD_DEPS STREQUAL "yes")
    message(STATUS "Downloading test resource - token_offsets.txt")
    file(DOWNLOAD https://gist.githubusercontent.com/kishorenc/1d330714eb07019f210f16ccb3991217/raw/bd52e05375d305d5aaa7ac06219af999726933a4/token_offsets.log ${TOKEN_OFFSETS_PATH})
endif()
