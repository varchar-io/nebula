find_package(Threads REQUIRED)

if(APPLE)
  set(GFLAGS_INCLUDE_DIRS ${OPT_DIR}/gflags/include)
  set(GFLAGS_LIBRARY_PATH ${OPT_DIR}/gflags/lib/libgflags.dylib)
else()
  set(COM_LIB_DIR /usr/lib/x86_64-linux-gnu)
  set(GFLAGS_INCLUDE_DIRS /usr/include)
  set(GFLAGS_LIBRARY_PATH ${COM_LIB_DIR}/libgflags.a)
endif()

set(GFLAGS_LIBRARY libgflags)
add_library(${GFLAGS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GFLAGS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GFLAGS_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GFLAGS_INCLUDE_DIRS}")
add_dependencies(${GFLAGS_LIBRARY} gflags)