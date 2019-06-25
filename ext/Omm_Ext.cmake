# open multi-methods implementation
# https://github.com/jll63/yomm2.git

find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
ExternalProject_Add(
  yomm2
  GIT_REPOSITORY https://github.com/jll63/yomm2.git
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(yomm2 SOURCE_DIR)
ExternalProject_Get_Property(yomm2 BINARY_DIR)
set(OMM_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${OMM_INCLUDE_DIRS})
set(OMM_LIBRARY_PATH ${BINARY_DIR}/src/${CMAKE_FIND_LIBRARY_PREFIXES}yomm2.a)
set(OMM_LIBRARY omm)
add_library(${OMM_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${OMM_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${OMM_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${OMM_INCLUDE_DIRS}")
add_dependencies(${OMM_LIBRARY} yomm2)