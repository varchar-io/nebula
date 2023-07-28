
find_package(Threads REQUIRED)

# not sure why we only add this for APPLE only.
include(ExternalProject)
SET(JSON_OPTS
  -DRAPIDJSON_BUILD_DOC=OFF
  -DRAPIDJSON_BUILD_TESTS=OFF
  -DRAPIDJSON_BUILD_CXX11=OFF
  -DRAPIDJSON_HAS_STDSTRING=ON)
ExternalProject_Add(rapidjson
  PREFIX rapidjson
  GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
  GIT_TAG a95e013b97ca6523f32da23f5095fcc9dd6067e5
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  BUILD_COMMAND ""
  CMAKE_ARGS ${JSON_OPTS}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(rapidjson SOURCE_DIR)
ExternalProject_Get_Property(rapidjson BINARY_DIR)
set(RAPIDJSON_INCLUDE_DIR ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${RAPIDJSON_INCLUDE_DIR})
set(JSON_LIBRARY json)
add_library(${JSON_LIBRARY} INTERFACE)
set_target_properties(${JSON_LIBRARY} PROPERTIES
    "INTERFACE_INCLUDE_DIRECTORIES" "${RAPIDJSON_INCLUDE_DIR}")
add_dependencies(${JSON_LIBRARY} rapidjson)
