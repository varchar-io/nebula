# tool version minimum requirement
cmake_minimum_required(VERSION 3.14)

# install latest cmake on linux
# check the version/build to use
# wget https://github.com/Kitware/CMake/releases/download/v3.15.2/cmake-3.15.2.tar.gz
# tar -xzvf cmake-3.15.2.tar.gz
# cd cmake-3.15.2/
# ./bootstrap
# make -j36
# sudo make install
# cmake --version (check version after installation)

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

# a switch to use clang or g++
# clang or g++
# this switch only works on linux
set(NC gcc)
if(APPLE)
  set(NCXX /usr/bin/g++)
  set(NCC /usr/bin/gcc)
  set(NCFLAGS "-Wall -Wextra -Werror -Wno-error=inconsistent-missing-override -Wno-error=nullability-completeness -Wno-error=sign-compare -Wno-error=unknown-warning-option")
  set(NCFLAGS_RELEASE "-O3")
else()
  if(${NC} STREQUAL "clang")
    set(NCXX /usr/bin/clang++)
    set(NCC /usr/bin/clang)
    set(NCFLAGS "-Wall -Wextra -Werror -I/usr/include/ -I/usr/local/include")
    set(NCFLAGS_RELEASE "-O3")
  else()
    # this build branch is the final release version
    set(NCXX /usr/bin/g++)
    set(NCC /usr/bin/gcc)
    list(APPEND LNCFLAGS 
      "-Wall -Wextra -Werror -lstdc++ -lstdc++fs"
      "-Wl,--no-as-needed -no-pie -ldl"
      "-I/usr/include/ -I/usr/local/include -L/usr/local/lib -L/usr/lib")

    # in my testing with aggregation func, tcmalloc performs better than jemalloc
    # turn it on as default for now
    if(NOT PPROF)
      set(PPROF 2)
    endif()

    # build flag -DGPROF
    if (GPROF STREQUAL "1")
      # using gprof  
      list(APPEND LNCFLAGS 
        "-pg -g"
        "-DGPROF")
    # build flag -DPPROF -> gperftools
    elseif(PPROF STREQUAL "1")
      # or using gperftools, ask each target to depends on perftools_library for linking
      # unwind is needed by gperftools
      list(APPEND LNCFLAGS
      "-lunwind"
      "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -g"
      "-DPPROF")
    elseif(PPROF STREQUAL "2")
      # or using gperftools, ask each target to depends on perftools_library for linking
      # unwind is needed by gperftools
      list(APPEND LNCFLAGS
      "-lunwind"
      "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -s")
    else()
      # strip symbols since we don't need profiling and use jemalloc by default on linux release
      # MALLOC_CONF=stats_print:true ./NodeServer to enable jemalloc stats  
      list(APPEND LNCFLAGS 
      "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
    endif()

    string(REPLACE ";" " " NCFLAGS "${LNCFLAGS}")
    # -Ofast vs -O3
    # HashFlat.genCopier will crash by calling ValueEval.merge for int128 typed avg function
    # I have spent much time to figure out why but no luck yet, need some more help in the future.
    # optimizations working fine: -O0, -O1, -Os
    # -O2, -O3, -Ofast will crash
    # Perf Benchmark(6 tests): ./ApiTests --gtest_filter=ApiTest.*:-ApiTest.TestAvgAggregation
    # interesting, by running an end2end query test, -O1 actually runs faster than -O2 on the same box.
    # also try optimizations for size (Os)
    # -O1: 800ms, 797ms, 807ms, 808ms, 801ms
    # -O2: 892ms, 871ms, 898ms, 881ms, 871ms
    # -O3: 608ms, 594ms, 605ms, 600ms, 599ms
    # -Ofast: 596ms, 608ms, 596ms, 594ms, 603ms
    # -Os: 861ms, 861ms, 865ms, 866ms, 843ms
    # figure out flags differences between optimization level:
    # dump flags for o1: gcc -O1 -Q --help=optimizers -xc - < /dev/null > o1.txt
    # dump flags for o2: gcc -O2 -Q --help=optimizers -xc - < /dev/null > o2.txt
    # diff them
    set(NCFLAGS_RELEASE "-Ofast -march=native")
  endif()
endif()

# by default using clang, turn this line on to use gcc
# link g++-8 on linux to default g++ / gcc 
# sudo apt-get update
# sudo apt-get install gcc-9 g++-9
# sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 800 --slave /usr/bin/g++ g++ /usr/bin/g++-9
# install new version clang on ubuntu
# # Install/configure the compiler to use for Envoy.
# > sudo su
# > wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
# > apt-add-repository "deb https://apt.llvm.org/xenial/ llvm-toolchain-xenial-8 main" && \
# > apt-get -q -y update && \
# > apt-get -q -y --no-install-recommends install \
# >   clang-8 \
# >   clang-format-8 \
# >   clang-tidy-8 \
# >   lld-8 \
# >   lldb-8 \
# >   libc++-8-dev \
# >   libc++abi-8-dev
# (links)
# update-alternatives --install /usr/bin/cc cc /usr/bin/clang-8 100
# update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-8 100
# update-alternatives --install /usr/bin/clang clang /usr/bin/clang-8 800 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-8
# test clang 8
set(CMAKE_CXX_COMPILER ${NCXX})
set(CMAKE_CC_COMPILER ${NCXX})
set(CMAKE_C_COMPILER ${NCC})

if(NOT CMAKE_BUILD_TYPE)
  if(APPLE)
    set(CMAKE_BUILD_TYPE Debug)
  else()
    set(CMAKE_BUILD_TYPE Release)
  endif()
endif()

set(CMAKE_CXX_FLAGS ${NCFLAGS})
set(CMAKE_CXX_FLAGS_DEBUG "-g -DNDBG")
set(CMAKE_CXX_FLAGS_RELEASE ${NCFLAGS_RELEASE})

# we use static link always
set(BUILD_SHARED_LIBS OFF)

# set cellar path for libries installed by brew
if(APPLE)
  set(DL_EXT "dylib")
  set(CELLAR_ROOT /usr/local/Cellar)
else()
  set(DL_EXT "so")
  set(CELLAR_ROOT /home/linuxbrew/.linuxbrew/Cellar)
endif()

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

# add dependency of compression
include(ext/Compression_Ext.cmake)

# add a dependency of glog
include(ext/Gflags_Ext.cmake)

# add a dependency of glog
include(ext/Glog_Ext.cmake)

# double conversion
include(ext/Doubleconversion_Ext.cmake)

# lib event
include(ext/Libevent_Ext.cmake)

# include highway for SIMD
include(ext/Highway_Ext.cmake)

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

# add boost modules definition
include(ext/Boost_Ext.cmake)

# add open ssl definition
include(ext/Openssl_Ext.cmake)

# add thrift module
include(ext/Thrift_Ext.cmake)

# include pre-installed folly
include(ext/Folly_Ext.cmake)

# add a dependency of cuckoo filter
include(ext/Cuckoo_Ext.cmake)

# include AWS external project
include(ext/Aws_Ext.cmake)

# include protobuf
include(ext/Protobuf_Ext.cmake)

# include GRPC external project
include(ext/Grpc_Ext.cmake)

# include rapid JSON
include(ext/Json_Ext.cmake)

# include open multi-methods
include(ext/Omm_Ext.cmake)

# include arrow
include(ext/Arrow_Ext.cmake)

# include xxhash
include(ext/Xxh_Ext.cmake)

# include yaml
include(ext/Yaml_Ext.cmake)

# include uri parser
include(ext/Uri_Ext.cmake)

# include msgpack - tuples serde
include(ext/Msgpack_Ext.cmake)

# include perf tools
include(ext/Perf_Ext.cmake)

#include kafka library
include(ext/Kafka_Ext.cmake)

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

# build data surface - interfaces
include(src/surface/Surface.cmake)

# build type library
include(src/type/Type.cmake)

# build meta module
include(src/meta/Meta.cmake)

# build memory library
include(src/memory/Memory.cmake)

# build data storage - clound providers
include(src/storage/Storage.cmake)

# build data ingestion
include(src/ingest/Ingest.cmake)

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

# enable testing so that we can run "make test"
enable_testing()

# a command to install clang-format - not related to build
# execute_process(COMMAND npm install clang-format)

# ensure all resources copied to build folder
# all resources are relative to nebula/src folder only
set(NRES 
  configs/test.yml 
  configs/cluster.yml)
foreach(i ${NRES})
  configure_file(${NEBULA_SRC}/${i} ${CMAKE_CURRENT_BINARY_DIR}/${i} COPYONLY)
endforeach()
