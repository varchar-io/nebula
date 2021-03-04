find_package(Threads REQUIRED)

include(ExternalProject)
SET(DC_OPTS
  -DBUILD_TESTING=OFF)
ExternalProject_Add(double-conversion
  PREFIX double-conversion
  GIT_REPOSITORY https://github.com/google/double-conversion.git
  GIT_TAG v3.1.5
  CMAKE_ARGS ${DC_OPTS}
  INSTALL_DIR ${NEBULA_INSTALL}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

ExternalProject_Get_Property(double-conversion SOURCE_DIR)
ExternalProject_Get_Property(double-conversion BINARY_DIR)

# define double-conversion
set(DC_INCLUDE_DIRS ${SOURCE_DIR})
set(DC_LIBRARY_PATH ${BINARY_DIR}/libdouble-conversion.a)

set(DC_LIBRARY libdc)
add_library(${DC_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${DC_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${DC_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${DC_INCLUDE_DIRS}")