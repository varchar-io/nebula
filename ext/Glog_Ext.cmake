find_package(Threads REQUIRED)

include(ExternalProject)
ExternalProject_Add(glog
  PREFIX glog
  GIT_REPOSITORY https://github.com/google/glog.git
  GIT_TAG v0.4.0
  UPDATE_COMMAND ""
  INSTALL_DIR ${NEBULA_INSTALL}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

ExternalProject_Get_Property(glog SOURCE_DIR)
ExternalProject_Get_Property(glog BINARY_DIR)

set(GLOG_INCLUDE_DIRS ${SOURCE_DIR}/src ${BINARY_DIR})
file(MAKE_DIRECTORY ${GLOG_INCLUDE_DIRS})
set(GLOG_LIBRARY_PATH ${BINARY_DIR}/libglog.a)
set(GLOG_LIBRARY libglog)
add_library(${GLOG_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GLOG_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GLOG_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GLOG_INCLUDE_DIRS}")
add_dependencies(${GLOG_LIBRARY} glog)
