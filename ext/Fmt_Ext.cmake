# https://cmake.org/cmake/help/latest/module/ExternalProject.html
find_package(Threads REQUIRED)

if(APPLE)
  set(FMT_ROOT ${CELLAR_ROOT}/fmt/6.1.2)
  set(FMT_INCLUDE_DIRS ${FMT_ROOT}/include)
  set(FMT_LIBRARY_PATH ${FMT_ROOT}/lib/libfmt.a)
else()
  include(ExternalProject)
  ExternalProject_Add(fmt
    PREFIX fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

  # get source dir after download step
  ExternalProject_Get_Property(fmt SOURCE_DIR)
  ExternalProject_Get_Property(fmt BINARY_DIR)
  set(FMT_INCLUDE_DIR ${SOURCE_DIR}/include)
  file(MAKE_DIRECTORY ${FMT_INCLUDE_DIR})
  set(FMT_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}fmt.a)
endif()

set(FMT_LIBRARY libfmt)
add_library(${FMT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${FMT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${FMT_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${FMT_INCLUDE_DIR}")
add_dependencies(${FMT_LIBRARY} fmt)
