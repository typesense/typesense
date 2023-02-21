project(onnxruntme_ext)
set(ONNX_EXT_NAME onnxruntime_ext)
include(ExternalProject)

if(NOT EXISTS ${DEP_ROOT_DIR}/${ONNX_EXT_NAME})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${ONNX_EXT_NAME})
else()
    file(REMOVE_RECURSE ${DEP_ROOT_DIR}/${ONNX_EXT_NAME})
    file(MAKE_DIRECTORY ${DEP_ROOT_DIR}/${ONNX_EXT_NAME})
endif()

ExternalProject_Add(
    onnxruntime_ext
    GIT_REPOSITORY https://github.com/microsoft/onnxruntime-extensions
    GIT_TAG origin/rel-0.6.0
    SOURCE_DIR ${DEP_ROOT_DIR}/${ONNX_EXT_NAME}
    PATCH_COMMAND cd ${DEP_ROOT_DIR}/${ONNX_EXT_NAME} && git apply ${CMAKE_CURRENT_SOURCE_DIR}/cmake/onnx_ext.patch || git apply ${CMAKE_CURRENT_SOURCE_DIR}/cmake/onnx_ext.patch -R --check && cd operators && mkdir -p src_dir && mkdir -p src_dir/tokenizer && cp base64.h base64.cc string_utils_onnx.h string_utils_onnx.cc ustring.h ustring.cc src_dir && cp tokenizer/bert_tokenizer.hpp tokenizer/bert_tokenizer.cc tokenizer/basic_tokenizer.hpp tokenizer/basic_tokenizer.cc src_dir/tokenizer 
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)
