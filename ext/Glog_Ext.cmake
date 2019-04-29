find_package(Threads REQUIRED)

if(APPLE)
  include(ExternalProject)
  SET(GLOG_OPTS
    -DWITH_GFLAGS:BOOL=OFF)

  ExternalProject_Add(
    glogp
    GIT_REPOSITORY https://github.com/google/glog.git
    CMAKE_ARGS ${GLOG_OPTS}
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

  # get source dir after download step
  ExternalProject_Get_Property(glogp BINARY_DIR)
  set(GLOG_INCLUDE_DIRS ${BINARY_DIR})
  set(GLOG_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}glog.a)
else()
  set(GLOG_INCLUDE_DIRS /usr/local/include)
  set(GLOG_LIBRARY_PATH /usr/local/lib/libglog.a)
endif()

set(GLOG_LIBRARY glog)
add_library(${GLOG_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GLOG_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GLOG_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GLOG_INCLUDE_DIRS}")
add_dependencies(${GLOG_LIBRARY} glogp)
