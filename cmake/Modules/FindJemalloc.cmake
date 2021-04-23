# Tries to find Jemalloc headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(jemalloc)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  JEMALLOC_ROOT_DIR  Set this variable to the root installation of
#                    Jemalloc if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  JEMALLOC_FOUND              System has Jemalloc libs/headers
#  JEMALLOC_LIBRARIES          The Jemalloc libraries
#  JEMALLOC_INCLUDE_DIR        The location of Jemalloc headers

find_path(JEMALLOC_INCLUDE_DIR
        NAMES jemalloc.h
        HINTS ${JEMALLOC_ROOT_DIR}/include/jemalloc
        NO_DEFAULT_PATH)

find_library(JEMALLOC_LIBRARIES
        NAMES jemalloc
        HINTS ${JEMALLOC_ROOT_DIR}/lib
        NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jemalloc DEFAULT_MSG
        JEMALLOC_LIBRARIES
        JEMALLOC_INCLUDE_DIR)

mark_as_advanced(
        JEMALLOC_ROOT_DIR
        JEMALLOC_LIBRARIES
        JEMALLOC_INCLUDE_DIR)
