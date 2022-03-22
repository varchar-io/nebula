find_package(Threads REQUIRED)
include(ExternalProject)

# install re2 linked by grpc
ExternalProject_Add(re2
  PREFIX re2
  GIT_REPOSITORY https://github.com/google/re2.git
  GIT_TAG 2021-09-01
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

ExternalProject_Get_Property(re2 SOURCE_DIR)
ExternalProject_Get_Property(re2 BINARY_DIR)

set(RE2_INCLUDE_DIRS ${SOURCE_DIR}/re2)
file(MAKE_DIRECTORY ${RE2_INCLUDE_DIRS})
set(RE2_LIBRARY_PATH ${BINARY_DIR}/libre2.a)
set(RE2_LIBRARY libre2)
add_library(${RE2_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${RE2_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${RE2_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${RE2_INCLUDE_DIRS}")
add_dependencies(${RE2_LIBRARY} re2)
