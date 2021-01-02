find_package(Threads REQUIRED)

# not sure why we only add this for APPLE only.
include(ExternalProject)
ExternalProject_Add(gperftools
    PREFIX gperftools
    GIT_REPOSITORY https://github.com/gperftools/gperftools.git
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    BUILD_IN_SOURCE ON
    BUILD_COMMAND ./autogen.sh && ./configure && make install
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

ExternalProject_Get_Property(gperftools SOURCE_DIR)
if(NOT EXISTS ${SOURCE_DIR}/lib)
    add_custom_target(mgperftools ALL
        COMMAND ./autogen.sh && ./configure --prefix=${SOURCE_DIR} && make install -j8
        WORKING_DIRECTORY ${SOURCE_DIR}
        DEPENDS gperftools)
endif()

set(PERF_INCLUDE_DIR ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${PERF_INCLUDE_DIR})
set(PERF_LIBRARY_PATH ${SOURCE_DIR}/lib/libtcmalloc_and_profiler.a)
set(PERF_LIBRARY libperf)
add_library(${PERF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${PERF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${PERF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${PERF_INCLUDE_DIR}")
add_dependencies(${PERF_LIBRARY} mgperftools)