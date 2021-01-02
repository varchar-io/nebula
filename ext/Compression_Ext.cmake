# include system provided compression libs
# if not system available, follow instructions to install them
# zlib
set(COM_LIB_DIR /usr/lib/x86_64-linux-gnu)
if(APPLE)
    set(ZLIB_ROOT /usr/local/opt/zlib)
    set(ZLIB_INCLUDE_DIR ${ZLIB_ROOT}/include)
    set(ZLIB_LIBRARY_PATH ${ZLIB_ROOT}/lib/libz.a)
else()
    set(ZLIB_INCLUDE_DIR /usr/include)
    set(ZLIB_LIBRARY_PATH ${COM_LIB_DIR}/libz.a)
endif()
set(ZLIB_LIBRARY zlib)
add_library(${ZLIB_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${ZLIB_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${ZLIB_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${ZLIB_INCLUDE_DIR}")

# snappy - linux has system provided version
if(APPLE)
    set(SNAPPY_ROOT /usr/local/opt/snappy)
    set(SNAPPY_INCLUDE_DIR ${SNAPPY_ROOT}/include)
    set(SNAPPY_LIBRARY_PATH ${SNAPPY_ROOT}/lib/libsnappy.a)
else()
    set(SNAPPY_INCLUDE_DIR /usr/include)
    set(SNAPPY_LIBRARY_PATH ${COM_LIB_DIR}/libsnappy.a)
endif()
set(SNAPPY_LIBRARY snappy)
add_library(${SNAPPY_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${SNAPPY_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${SNAPPY_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${SNAPPY_INCLUDE_DIR}")

# zstd
if(APPLE)
    set(ZSTD_ROOT /usr/local/opt/zstd)
    set(ZSTD_INCLUDE_DIR ${ZSTD_ROOT}/include)
    set(ZSTD_LIBRARY_PATH ${ZSTD_ROOT}/lib/libzstd.a)
else()
    set(ZSTD_INCLUDE_DIR /usr/include)
    set(ZSTD_LIBRARY_PATH ${COM_LIB_DIR}/libzstd.a)
endif()
set(ZSTD_LIBRARY zstd)
add_library(${ZSTD_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${ZSTD_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${ZSTD_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${ZSTD_INCLUDE_DIR}")

# lz4
if(APPLE)
    set(LZ4_ROOT /usr/local/opt/lz4)
    set(LZ4_INCLUDE_DIR ${LZ4_ROOT}/include)
    set(LZ4_LIBRARY_PATH ${LZ4_ROOT}/lib/liblz4.a)
else()
    set(LZ4_INCLUDE_DIR /usr/include)
    set(LZ4_LIBRARY_PATH ${COM_LIB_DIR}/liblz4.a)
endif()
set(LZ4_LIBRARY lz4)
add_library(${LZ4_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${LZ4_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${LZ4_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${LZ4_INCLUDE_DIR}")

# bzip2
# bzip2 can be installed through brew on mac
# and apt-get install bzip2 on unbuntu
if(APPLE)
    set(BZ2_ROOT /usr/local/opt/bzip2)
    set(BZ2_INCLUDE_DIR ${BZ2_ROOT}/include)
    set(BZ2_LIBRARY_PATH ${BZ2_ROOT}/lib/libbz2.a)
else()
    set(BZ2_INCLUDE_DIR /usr/include)
    set(BZ2_LIBRARY_PATH ${COM_LIB_DIR}/libbz2.a)
endif()
set(BZ2_LIBRARY bz2)
add_library(${BZ2_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${BZ2_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${BZ2_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${BZ2_INCLUDE_DIR}")

# lzma
if(APPLE)
    set(LZMA_ROOT ${CELLAR_ROOT}/xz/5.2.5/)
    set(LZMA_INCLUDE_DIR ${LZMA_ROOT}/include)
    set(LZMA_LIBRARY_PATH ${LZMA_ROOT}/lib/liblzma.a)
else()
    set(LZMA_INCLUDE_DIR /usr/include)
    set(LZMA_LIBRARY_PATH ${COM_LIB_DIR}/liblzma.a)
endif()
set(LZMA_LIBRARY lzma)
add_library(${LZMA_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${LZMA_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${LZMA_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${LZMA_INCLUDE_DIR}")