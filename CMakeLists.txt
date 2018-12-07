# tool version minimum requirement
cmake_minimum_required(VERSION 3.13)

# define project meta
project(nebula)

# set compiler features - use C++ 11 and above
# Valid values after CMake 3.8 for CMAKE_CXX_STANDARD are 98, 11 and 14, with 17
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set_property(GLOBAL PROPERTY CXX_STANDARD 11)

# add a basic gtest dependency
include(ext/Gtest_Ext.cmake)

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})

# Main Test entry point
add_executable(MainTest src/main.cpp)

# install it into local bin folder
install(TARGETS MainTest DESTINATION bin)

# include google test module and build tests executable
enable_testing()
add_executable(tests test/usegtest.cpp)
target_link_libraries(tests ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})

include(GoogleTest)
gtest_discover_tests(tests TEST_PREFIX MATH)
