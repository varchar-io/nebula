if(PPROF)
    # using gperf tools with tcmalloc
    set(PERF_LIBRARY_PATH /usr/local/lib/libtcmalloc_and_profiler.a)
else()
    # use jemalloc even it is not a profiler
    set(PERF_LIBRARY_PATH /usr/local/lib/libjemalloc.a)
endif()
set(PERF_INCLUDE_DIR /usr/local/include)
set(PERF_LIBRARY perf)
add_library(${PERF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${PERF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${PERF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${PERF_INCLUDE_DIR}")
