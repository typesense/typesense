# Download test resources

set(ART_VERSION bbbf588bca55bce095538ee8ca8b422904baebc5)
set(ART_WORDS_PATH /tmp/typesense_test/words.txt)
set(ART_UUID_PATH /tmp/typesense_test/uuid.txt)

if(NOT EXISTS /tmp/typesense_test)
    file(MAKE_DIRECTORY /tmp/typesense_test)
endif()

if(NOT EXISTS ${ART_WORDS_PATH})
    message(STATUS "Downloading test resource - words.txt")
    file(DOWNLOAD https://raw.githubusercontent.com/kishorenc/libart/${ART_VERSION}/tests/words.txt ${ART_WORDS_PATH})
endif()

if(NOT EXISTS ${ART_UUID_PATH})
    message(STATUS "Downloading test resource - uuid.txt")
    file(DOWNLOAD https://raw.githubusercontent.com/kishorenc/libart/${ART_VERSION}/tests/uuid.txt ${ART_UUID_PATH})
endif()