# use uriparser to parse uri
find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
SET(MSGPACK_OPTS
  -DBUILD_SHARED_LIBS=OFF
  -DMSGPACK_BUILD_TESTS=OFF
  -DMSGPACK_BUILD_DOCS=OFF
  -DCMAKE_BUILD_TYPE=Release)
ExternalProject_Add(msgpack
  PREFIX msgpack
  GIT_REPOSITORY https://github.com/msgpack/msgpack-c.git
  CMAKE_ARGS ${MSGPACK_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(msgpack SOURCE_DIR)
ExternalProject_Get_Property(msgpack BINARY_DIR)

set(MSGPACK_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${MSGPACK_INCLUDE_DIRS})
set(MSGPACK_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}msgpackc.a)
set(MSGPACK_LIBRARY libmsgpack)
add_library(${MSGPACK_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${MSGPACK_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${MSGPACK_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${MSGPACK_INCLUDE_DIRS}")
add_dependencies(${MSGPACK_LIBRARY} msgpack)
