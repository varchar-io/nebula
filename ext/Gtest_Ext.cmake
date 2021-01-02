find_package(Threads REQUIRED)

include(ExternalProject)
# the commit is working as I tested 
# while official version doesn't such as "release-1.10.0"
# so keep it until we test new version
ExternalProject_Add(gtest
    PREFIX gtest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG 389cb68b87193358358ae87cc56d257fd0d80189
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

ExternalProject_Get_Property(gtest SOURCE_DIR)
ExternalProject_Get_Property(gtest BINARY_DIR)
set(GTEST_INCLUDE_DIRS ${SOURCE_DIR}/googletest/include)
file(MAKE_DIRECTORY ${GTEST_INCLUDE_DIRS})
set(GMOCK_INCLUDE_DIRS ${SOURCE_DIR}/googlemock/include)
file(MAKE_DIRECTORY ${GMOCK_INCLUDE_DIRS})
set(gtest_root ${BINARY_DIR})

set(GTEST_LIBRARY_PATH ${gtest_root}/lib/libgtest.a)
set(GTEST_LIBRARY libgtest)
add_library(${GTEST_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GTEST_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GTEST_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GTEST_INCLUDE_DIRS}")
add_dependencies(${GTEST_LIBRARY} gtest)

set(GTEST_MAIN_LIBRARY_PATH ${gtest_root}/lib/libgtest_main.a)
set(GTEST_MAIN_LIBRARY gtest_main)
add_library(${GTEST_MAIN_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GTEST_MAIN_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GTEST_MAIN_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GTEST_INCLUDE_DIRS}")
add_dependencies(${GTEST_MAIN_LIBRARY} gtest)

set(GMOCK_LIBRARY_PATH ${gtest_root}/lib/libgmock.a)
set(GMOCK_LIBRARY libgmock)
add_library(${GMOCK_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GMOCK_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GMOCK_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GMOCK_INCLUDE_DIRS}")
add_dependencies(${GMOCK_LIBRARY} gtest)

set(GMOCK_MAIN_LIBRARY_PATH ${gtest_root}/lib/libgmock_main.a)
set(GMOCK_MAIN_LIBRARY gmock_main)
add_library(${GMOCK_MAIN_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GMOCK_MAIN_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GMOCK_MAIN_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GMOCK_INCLUDE_DIRS}")
add_dependencies(${GMOCK_MAIN_LIBRARY} gtest)