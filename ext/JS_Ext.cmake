# https://bellard.org/quickjs/
# By super hacker Fabrice Bellard
# under MIT lic.

find_package(Threads REQUIRED)

# Nebula runs an embeded JS engine to support customized functions through JS
# This is very important feature to allow user plugin data processing logic
# We have a bunch of choices such as Chrome V8, Mozilla SpiderMonkey
# But this awesome lightweight quickjs is the first thing we're trying.
# Unless we see performance issue - this is a very good fit.
# Current commit 204682fb87ab9312f0cf81f959ecd181180457bc @ 11/08
include(ExternalProject)
ExternalProject_Add(quickjs
    PREFIX quickjs
    GIT_REPOSITORY https://github.com/bellard/quickjs
    GIT_TAG 204682fb87ab9312f0cf81f959ecd181180457bc
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(quickjs SOURCE_DIR)

# non-cmake project, build it by make
# to make build pass on MAC, there is a patch we need to manually apply
# in quickjs-libc.c header area:
# // Apple doesn't provide the environ global variable
# #if defined(__APPLE__) && !defined(environ)
#   #include <crt_externs.h>
#   #define environ (*_NSGetEnviron())
# #endif
add_custom_target(makeqjs ALL
        COMMAND make
        WORKING_DIRECTORY ${SOURCE_DIR}
        DEPENDS quickjs)

set(QJS_INCLUDE_DIRS ${SOURCE_DIR})
file(MAKE_DIRECTORY ${QJS_INCLUDE_DIRS})
# NOTE: LATEST boost such as 1.75.0 will conflict with version file
# either ensure boost old version or we can remove this useless file
file(REMOVE ${SOURCE_DIR}/version)

set(QJS_LIBRARY_PATH ${SOURCE_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}quickjs.a)
set(QJS_LIBRARY libqjs)
add_library(${QJS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${QJS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${QJS_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${QJS_INCLUDE_DIRS}")

# linker time optimization version?
# set(QJS_LTO_LIBRARY_PATH ${SOURCE_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}quickjs.lto.a)
# set(QJS_LTO_LIBRARY libqjslto)
# add_library(${QJS_LTO_LIBRARY} UNKNOWN IMPORTED)
# set_target_properties(${QJS_LTO_LIBRARY} PROPERTIES
#     "IMPORTED_LOCATION" "${QJS_LTO_LIBRARY_PATH}"
#     "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
#     "INTERFACE_INCLUDE_DIRECTORIES" "${QJS_INCLUDE_DIRS}")

add_dependencies(${QJS_LIBRARY} makeqjs)
