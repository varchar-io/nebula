# use uriparser to parse uri
find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)

SET(URIP_OPTS
  -DBUILD_SHARED_LIBS=OFF
  -DURIPARSER_BUILD_TESTS=OFF
  -DURIPARSER_BUILD_DOCS=OFF
  -DCMAKE_BUILD_TYPE=Release)

ExternalProject_Add(uriparser
  PREFIX uriparser
  GIT_REPOSITORY https://github.com/uriparser/uriparser.git
  CMAKE_ARGS ${URIP_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(uriparser SOURCE_DIR)
ExternalProject_Get_Property(uriparser BINARY_DIR)

set(URIP_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${URIP_INCLUDE_DIRS})
set(URIP_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}uriparser.a)
set(URIP_LIBRARY liburiparser)
add_library(${URIP_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${URIP_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${URIP_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${URIP_INCLUDE_DIRS}")
add_dependencies(${URIP_LIBRARY} uriparser)
