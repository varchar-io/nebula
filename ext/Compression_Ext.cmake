# include system provided compression libs
# if not system available, follow instructions to install them
# zlib
if(APPLE)
    set(ZLIB_ROOT ${CELLAR_ROOT}/zlib/1.2.11)
    set(ZLIB_INCLUDE_DIR ${ZLIB_ROOT}/include)
    set(ZLIB_LIBRARY_PATH ${ZLIB_ROOT}/lib/libz.a)
else()
    set(ZLIB_INCLUDE_DIR /usr/include)
    set(ZLIB_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libz.a)
endif()
set(ZLIB_LIBRARY zlib)
add_library(${ZLIB_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${ZLIB_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${ZLIB_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${ZLIB_INCLUDE_DIR}")

# snappy - linux has system provided version
if(APPLE)
    set(SNAPPY_ROOT ${CELLAR_ROOT}/snappy/1.1.7_1)
    set(SNAPPY_INCLUDE_DIR ${SNAPPY_ROOT}/include)
    set(SNAPPY_LIBRARY_PATH ${SNAPPY_ROOT}/lib/libsnappy.a)
else()
    set(SNAPPY_INCLUDE_DIR /usr/local/include)
    set(SNAPPY_LIBRARY_PATH /usr/local/lib/libsnappy.a)
endif()
set(SNAPPY_LIBRARY snappy)
add_library(${SNAPPY_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${SNAPPY_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${SNAPPY_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${SNAPPY_INCLUDE_DIR}")

# zstd
if(APPLE)
    set(ZSTD_ROOT ${CELLAR_ROOT}/zstd/1.4.1)
    set(ZSTD_INCLUDE_DIR ${ZSTD_ROOT}/include)
    set(ZSTD_LIBRARY_PATH ${ZSTD_ROOT}/lib/libzstd.a)
else()
    set(ZSTD_INCLUDE_DIR /usr/local/include)
    set(ZSTD_LIBRARY_PATH /usr/local/lib/libzstd.a)
endif()
set(ZSTD_LIBRARY zstd)
add_library(${ZSTD_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${ZSTD_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${ZSTD_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${ZSTD_INCLUDE_DIR}")

# lz4
if(APPLE)
    set(LZ4_ROOT ${CELLAR_ROOT}/lz4/1.9.1)
    set(LZ4_INCLUDE_DIR ${LZ4_ROOT}/include)
    set(LZ4_LIBRARY_PATH ${LZ4_ROOT}/lib/liblz4.a)
else()
    set(LZ4_INCLUDE_DIR /usr/local/include)
    set(LZ4_LIBRARY_PATH /usr/local/lib/liblz4.a)
endif()
set(LZ4_LIBRARY lz4)
add_library(${LZ4_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${LZ4_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${LZ4_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${LZ4_INCLUDE_DIR}")
