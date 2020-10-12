# XXHash - by Yann
# https://github.com/Cyan4973/xxHash.git

find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)

ExternalProject_Add(xxhash
  PREFIX xxhash
  GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
  GIT_TAG release
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(xxhash SOURCE_DIR)

set(MKARGS "-DXXH_STATIC_LINKING_ONLY -fPIC")
add_custom_target(makexxh ALL
      COMMAND make CFLAGS=${MKARGS}
      WORKING_DIRECTORY ${SOURCE_DIR}
      DEPENDS xxhash)

set(XXH_INCLUDE_DIRS ${SOURCE_DIR})
file(MAKE_DIRECTORY ${XXH_INCLUDE_DIRS})
set(XXH_LIBRARY_PATH ${SOURCE_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}xxhash.a)
set(XXH_LIBRARY libxxh)
add_library(${XXH_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${XXH_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${XXH_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${XXH_INCLUDE_DIRS}")
add_dependencies(${XXH_LIBRARY} xxhash makexxh)
