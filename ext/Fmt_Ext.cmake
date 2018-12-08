find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
ExternalProject_Add(
  fmtproj
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(fmtproj SOURCE_DIR)
ExternalProject_Get_Property(fmtproj BINARY_DIR)
set(FMT_INCLUDE_DIRS ${SOURCE_DIR}/include)
set(FMT_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}fmt.a)
set(FMT_LIBRARY fmt)
add_library(${FMT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${FMT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${FMT_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${FMT_INCLUDE_DIRS}")
add_dependencies(${FMT_LIBRARY} fmtproj)
