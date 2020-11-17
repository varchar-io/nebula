find_package(Threads REQUIRED)

# http://roaringbitmap.org/
# by default, roaring is providng dynamic lib for linking
# however I changed it to build static here - we may want to adjust in final deployment
include(ExternalProject)
ExternalProject_Add(roaring
  PREFIX roaring
  GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CMAKE_ARGS "-DROARING_BUILD_STATIC=ON"
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(roaring SOURCE_DIR)
ExternalProject_Get_Property(roaring BINARY_DIR)
set(ROARING_INCLUDE_DIRS ${SOURCE_DIR}/include ${SOURCE_DIR}/cpp)
file(MAKE_DIRECTORY ${ROARING_INCLUDE_DIRS})
set(ROARING_LIBRARY_PATH ${BINARY_DIR}/src/${CMAKE_FIND_LIBRARY_PREFIXES}roaring.a)
set(ROARING_LIBRARY libroaring)
add_library(${ROARING_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${ROARING_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${ROARING_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${ROARING_INCLUDE_DIRS}")
add_dependencies(${ROARING_LIBRARY} roaring)