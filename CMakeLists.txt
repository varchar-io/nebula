# tool version minimum requirement
cmake_minimum_required(VERSION 3.14)

# define project meta
project(nebula)
set(NEBULA_ROOT ${PROJECT_SOURCE_DIR})
set(NEBULA_SRC ${NEBULA_ROOT}/src)

# define all headers should seek under SRC 
# so that we have to use full namespace to reference them
# NOTE: there are different options to setup interface/imple separations
# one clean way is to organize all headers under {root}/include but 
# it will duplicate all the module's strucutre, hence here we choose to expose whole SRC
include_directories(include ${NEBULA_SRC})

# set compiler features - use C++ 17 and above
# Valid values after CMake 3.8 for CMAKE_CXX_STANDARD are 98, 11 and 14, with 17
# set_property(GLOBAL PROPERTY CXX_STANDARD 17)
# NOTE: sometimes, cmake has some stale cache which causes errors for "cmake .." like
# CMake Error in CMakeLists.txt:
#   Target "MainTest" requires the language dialect "CXX17" (with compiler
#   extensions), but CMake does not know the compile flags to use to enable it.
# To fix it, just remove all cmake related files in build folder
# rm -rf CMakeFiles && rm -rf *.cmake && rm CMakeCache.txt
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_VERBOSE_MAKEFILE on)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

if(APPLE)
  set(CMAKE_CXX_FLAGS "-Wall -Wextra -Werror -Wno-error=nullability-completeness")
else()
  set(CMAKE_CXX_FLAGS "-Wall -Wextra -Werror -lstdc++ -Wl,--no-as-needed -ldl -I/usr/local/include -L/usr/local/lib")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -s")

# we use static link always
set(BUILD_SHARED_LIBS OFF)

# by default using clang, turn this line on to use gcc
# link g++-8 on linux to default g++ / gcc 
# sudo apt-get update
# sudo apt-get install gcc-8 g++-8
# sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8
set(CMAKE_CXX_COMPILER /usr/bin/g++)
set(CMAKE_CC_COMPILER /usr/bin/g++)
set(CMAKE_C_COMPILER /usr/bin/gcc)

# TODO(cao) - move to jemalloc
# ref https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919/

# include basic boost library
# set(Boost_USE_STATIC_LIBS        ON) # only find static libs
# set(Boost_USE_MULTITHREADED      ON)
# set(Boost_USE_STATIC_RUNTIME    OFF)
# find_package(Boost 1.69.0)
# if(Boost_FOUND)
#   include_directories(${Boost_INCLUDE_DIRS})
#   # depenency as ${Boost_LIBRARIES}
# endif()

# add a dependency of glog
include(ext/Gflags_Ext.cmake)

# add a dependency of glog
include(ext/Glog_Ext.cmake)

# add a dependency of fmt 
include(ext/Fmt_Ext.cmake)

# add a dependency of bf
include(ext/Bloom_Ext.cmake)

# add a dependency of roaring
include(ext/Roaring_Ext.cmake)

# add a basic gtest dependency
include(ext/Gtest_Ext.cmake)

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})
include_directories(include ${GMOCK_INCLUDE_DIRS})

# include pre-installed folly
include(ext/Folly_Ext.cmake)

# add a dependency of cuckoo filter
include(ext/Cuckoo_Ext.cmake)

# include AWS external project
include(ext/Aws_Ext.cmake)

# include GRPC external project
include(ext/Grpc_Ext.cmake)

# include rapid JSON
include(ext/Json_Ext.cmake)

# include arrow
include(ext/Arrow_Ext.cmake)

# "brew install conan" first - it's great after migrating to conan
# at least for folly - thanks conan, how to solve conflicts with existing lib like gflags?.
# use conan to install the dependency
# and use CONAN_PKG::folly to reference installed module
# before that please run "conan install .." in build dir
# or "conan install <path of conanfile.txt>"
# include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
# conan_basic_setup(TARGETS)
# # use folly library that managed by conan
# set (FOLLY_LIBRARY  CONAN_PKG::folly)

# below are modules in current project: their order matters which aligns with dependency order.
# otherwise we may see linker error.

# build common library
include(src/common/Common.cmake)

# build data storage - clound providers
include(src/storage/Storage.cmake)

# build data surface - interfaces
include(src/surface/Surface.cmake)

# build type library
include(src/type/Type.cmake)

# build meta module
include(src/meta/Meta.cmake)

# build memory library
include(src/memory/Memory.cmake)

# build meta module
include(src/execution/Execution.cmake)

# build api library
include(src/api/Api.cmake)

# build services
include(src/service/Service.cmake)

# Main Test entry point
add_executable(MainTest src/main.cpp)

# install it into local bin folder
install(TARGETS MainTest DESTINATION bin)

# include google test module and build tests executable
enable_testing()
add_executable(tests test/usegtest.cpp)
target_link_libraries(tests 
    ${FOLLY_LIBRARY}
    ${GTEST_LIBRARY} 
    ${GMOCK_LIBRARY} 
    ${GTEST_MAIN_LIBRARY})

# so that "make test" will execute all tests 
include(GoogleTest)
gtest_discover_tests(tests TEST_LIST ALL)

# a command to install clang-format - not related to build
# execute_process(COMMAND npm install clang-format)
