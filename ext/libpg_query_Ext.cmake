# not sure why we only add this for APPLE only.


set(LIBPG_QUERY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libpg_query)

set(LIBPG_QUERY_LIBRARY libpg_query)

include(ExternalProject)
ExternalProject_Add(pg_query
        GIT_REPOSITORY https://github.com/chenqin/libpg_query.git
        PREFIX pg_query
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND ""
        BUILD_COMMAND make
        INSTALL_COMMAND "")

# get source dir after download step
ExternalProject_Get_Property(pg_query SOURCE_DIR)
ExternalProject_Get_Property(pg_query BINARY_DIR)

set(LIBPG_QUERY_INCLUDE_DIR ${SOURCE_DIR})
file(MAKE_DIRECTORY ${LIBPG_QUERY_INCLUDE_DIR})
set(LIBPG_QUERY_LIBRARY_PATH ${BINARY_DIR}/libpg_query.a)

add_library(${LIBPG_QUERY_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${LIBPG_QUERY_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${LIBPG_QUERY_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${LIBPG_QUERY_INCLUDE_DIR}")