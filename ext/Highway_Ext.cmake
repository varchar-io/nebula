find_package(Threads REQUIRED)

include(ExternalProject)
ExternalProject_Add(highway
    PREFIX highway
    GIT_REPOSITORY https://github.com/google/highway.git
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

  # get source dir after download step
  ExternalProject_Get_Property(highway SOURCE_DIR)
  ExternalProject_Get_Property(highway BINARY_DIR)
  set(HWY_INCLUDE_DIRS ${SOURCE_DIR})
  file(MAKE_DIRECTORY ${HWY_INCLUDE_DIRS})
  set(HWY_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}hwy.a)
  set(HWY_LIBRARY libhwy)
  add_library(${HWY_LIBRARY} UNKNOWN IMPORTED)
  set_target_properties(${HWY_LIBRARY} PROPERTIES
      "IMPORTED_LOCATION" "${HWY_LIBRARY_PATH}"
      "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
      "INTERFACE_INCLUDE_DIRECTORIES" "${HWY_INCLUDE_DIRS}")
  add_dependencies(${HWY_LIBRARY} highway)
