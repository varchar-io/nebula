find_package(Threads REQUIRED)

if(APPLE)
  include(ExternalProject)
  ExternalProject_Add(
    gflagsp
    GIT_REPOSITORY https://github.com/gflags/gflags.git
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

  # get source dir after download step
  ExternalProject_Get_Property(gflagsp SOURCE_DIR)
  ExternalProject_Get_Property(gflagsp BINARY_DIR)
  set(GFLAGS_INCLUDE_DIRS ${BINARY_DIR}/include)
  file(MAKE_DIRECTORY ${GFLAGS_INCLUDE_DIRS})
  set(GFLAGS_LIBRARY_PATH ${BINARY_DIR}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gflags.a)

else()
  set(GFLAGS_INCLUDE_DIRS /usr/local/include)
  set(GFLAGS_LIBRARY_PATH /usr/local/lib/libgflags.a)
endif()

set(GFLAGS_LIBRARY gflags)
add_library(${GFLAGS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GFLAGS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GFLAGS_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GFLAGS_INCLUDE_DIRS}")
add_dependencies(${GFLAGS_LIBRARY} gflagsp)