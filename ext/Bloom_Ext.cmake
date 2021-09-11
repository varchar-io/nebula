find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
ExternalProject_Add(bloom
  PREFIX bloom
  GIT_REPOSITORY https://github.com/mavam/libbf.git
  GIT_TAG 5478275d8a4e9a5cc163b44c34517c515bd898ec
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(bloom SOURCE_DIR)
ExternalProject_Get_Property(bloom BINARY_DIR)
set(BF_INCLUDE_DIRS ${SOURCE_DIR})
set(BF_LIBRARY_PATH ${BINARY_DIR}/lib/libbf.dylib)

set(BF_LIBRARY bf)
add_library(${BF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${BF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${BF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${BF_INCLUDE_DIRS}")
add_dependencies(${BF_LIBRARY} bloom)
