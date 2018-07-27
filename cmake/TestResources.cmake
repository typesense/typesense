# Download test resources

set(ART_VERSION bbbf588bca55bce095538ee8ca8b422904baebc5)
set(TEST_RESOURCES_DIR ${CMAKE_SOURCE_DIR}/build/test_resources)
set(ART_WORDS_PATH ${TEST_RESOURCES_DIR}/words.txt)
set(ART_UUID_PATH ${TEST_RESOURCES_DIR}/uuid.txt)

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
