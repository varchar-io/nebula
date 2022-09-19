find_package(Threads REQUIRED)

include(ExternalProject)

ExternalProject_Add(fmt
  PREFIX fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 8.0.1
  UPDATE_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(fmt SOURCE_DIR)
ExternalProject_Get_Property(fmt BINARY_DIR)

set(FMT_INCLUDE_DIR ${SOURCE_DIR})
set(FMT_LIBRARY_PATH ${BINARY_DIR}/libfmt.a)

set(FMT_LIBRARY libfmt)
add_library(${FMT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${FMT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${FMT_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${FMT_INCLUDE_DIR}")