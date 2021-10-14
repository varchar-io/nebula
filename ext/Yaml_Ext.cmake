find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)

ExternalProject_Add(yaml
  PREFIX yaml
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG 0d9dbcfe8c0df699aed8ae050dddaca614178fb1
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(yaml SOURCE_DIR)
ExternalProject_Get_Property(yaml BINARY_DIR)

set(YAML_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${YAML_INCLUDE_DIRS})
set(YAML_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}yaml-cpp.a)
set(YAML_LIBRARY libyaml)
add_library(${YAML_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${YAML_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${YAML_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${YAML_INCLUDE_DIRS}")
add_dependencies(${YAML_LIBRARY} yaml)