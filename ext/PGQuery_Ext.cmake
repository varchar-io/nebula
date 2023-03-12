include(ExternalProject)
ExternalProject_Add(pg_query
    PREFIX pg_query
    GIT_REPOSITORY https://github.com/pganalyze/libpg_query.git
    GIT_TAG 15-4.2.0
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make
    INSTALL_COMMAND "")

# get source dir after download step
ExternalProject_Get_Property(pg_query SOURCE_DIR)
ExternalProject_Get_Property(pg_query BINARY_DIR)

set(LIBPG_QUERY_INCLUDE_DIR ${SOURCE_DIR} ${SOURCE_DIR}/postgres/src/include)
file(MAKE_DIRECTORY ${LIBPG_QUERY_INCLUDE_DIR})
set(LIBPG_QUERY_LIBRARY_PATH ${BINARY_DIR}/libpg_query.a)

set(LIBPG_QUERY_LIBRARY libpg_query)
add_library(${LIBPG_QUERY_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${LIBPG_QUERY_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${LIBPG_QUERY_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${LIBPG_QUERY_INCLUDE_DIR}")
