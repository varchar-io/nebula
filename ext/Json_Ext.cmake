if(APPLE)
    find_package(Threads REQUIRED)

  # http://roaringbitmap.org/
  # by default, roaring is providng dynamic lib for linking
  # however I changed it to build static here - we may want to adjust in final deployment
  include(ExternalProject)
  SET(JSON_OPTS
    -DRAPIDJSON_BUILD_DOC=OFF
    -DRAPIDJSON_BUILD_TESTS=OFF
    -DRAPIDJSON_BUILD_CXX11=OFF
    -DRAPIDJSON_HAS_STDSTRING=ON)
  ExternalProject_Add(
    rapidjson
    GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
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
  set(RAPIDJSON_INCLUDE_DIRS ${SOURCE_DIR}/include)
  file(MAKE_DIRECTORY ${RAPIDJSON_INCLUDE_DIRS})
  set(JSON_LIBRARY json)
  add_library(${JSON_LIBRARY} INTERFACE)
  set_target_properties(${JSON_LIBRARY} PROPERTIES
      "INTERFACE_INCLUDE_DIRECTORIES" "${RAPIDJSON_INCLUDE_DIRS}")
  add_dependencies(${JSON_LIBRARY} rapidjson)

endif()
