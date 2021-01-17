
find_package(Threads REQUIRED)

include(ExternalProject)
SET(FOLLY_OPTS
    -DFOLLY_HAVE_PREADV=OFF
    -DFOLLY_HAVE_PWRITEV=OFF
    -DFOLLY_USE_JEMALLOC=OFF)

if(APPLE)
    SET(FOLLY_OPTS 
        ${FOLLY_OPTS}
        -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl
        -DCMAKE_CXX_FLAGS=-L/usr/local/opt/icu4c/lib)
endif()

ExternalProject_Add(folly
  PREFIX folly
  GIT_REPOSITORY https://github.com/facebook/folly.git
  GIT_TAG v2020.09.21.00
  CMAKE_ARGS ${FOLLY_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  BUILD_COMMAND make -j8
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(folly SOURCE_DIR)
ExternalProject_Get_Property(folly BINARY_DIR)

set(FOLLY_INCLUDE_DIR ${SOURCE_DIR} ${BINARY_DIR})
set(FOLLY_LIBRARY_PATH ${BINARY_DIR}/libfolly.a)

set(FOLLY_LIBRARY libfolly)
add_library(${FOLLY_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${FOLLY_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${FOLLY_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${FOLLY_INCLUDE_DIR}")
add_dependencies(${FOLLY_LIBRARY} folly)

# define libiberty for linux
if(NOT APPLE)
    set(IBERTY_INCLUDE_DIRS /usr/local/include)
    set(IBERTY_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libiberty.a)
    set(IBERTY_LIBRARY iberty)
    add_library(${IBERTY_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${IBERTY_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${IBERTY_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${IBERTY_INCLUDE_DIRS}")
endif()

target_link_libraries(${FOLLY_LIBRARY}
    INTERFACE ${IBERTY_LIBRARY}
    INTERFACE ${DC_LIBRARY}
    INTERFACE ${EVENT_LIBRARY}
    INTERFACE ${Boost_LIBRARIES}
    INTERFACE ${ZLIB_LIBRARY}
    INTERFACE ${SNAPPY_LIBRARY}
    INTERFACE ${ZSTD_LIBRARY}
    INTERFACE ${LZ4_LIBRARY}
    INTERFACE ${BZ2_LIBRARY}
    INTERFACE ${LZMA_LIBRARY})
