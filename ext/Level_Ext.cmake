# use uriparser to parse uri
find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
SET(LEVELDB_OPTS
  -DLEVELDB_BUILD_TESTS=OFF
  -DLEVELDB_BUILD_BENCHMARKS=OFF
  -DLEVELDB_INSTALL=OFF)
ExternalProject_Add(leveldb
  PREFIX leveldb
  GIT_REPOSITORY https://github.com/google/leveldb.git
  CMAKE_ARGS ${LEVELDB_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(leveldb SOURCE_DIR)
ExternalProject_Get_Property(leveldb BINARY_DIR)

set(LEVELDB_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${LEVELDB_INCLUDE_DIRS})
set(LEVELDB_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}leveldb.a)
set(LEVELDB_LIBRARY libleveldb)
add_library(${LEVELDB_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${LEVELDB_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${LEVELDB_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${LEVELDB_INCLUDE_DIRS}")
add_dependencies(${LEVELDB_LIBRARY} leveldb)

target_link_libraries(${LEVELDB_LIBRARY}
    INTERFACE ${SNAPPY_LIBRARY})
