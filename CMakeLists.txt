# tool version minimum requirement
cmake_minimum_required(VERSION 3.14)

# define project meta
project(nebula)
set(NEBULA_ROOT ${PROJECT_SOURCE_DIR})
set(NEBULA_SRC ${NEBULA_ROOT}/src)
set(NEBULA_INSTALL ${NEBULA_ROOT}/build/install)
set(HOME_DIR $ENV{HOME})

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
set(CMAKE_VERBOSE_MAKEFILE ON)

# let all 3rd parties to be installed to here
set(CMAKE_INSTALL_PREFIX ${NEBULA_INSTALL})
set(CMAKE_PREFIX_PATH ${NEBULA_INSTALL})

# ON MAC, opt folder
set(OPT_DIR /usr/local/opt)

# a switch to use clang or g++
# clang or g++
# this switch only works on linux
set(NC gcc)
if(APPLE)
  if(ARM STREQUAL "1")
    set(OPT_DIR /opt/homebrew/opt)
  endif()

  set(NCXX /usr/bin/g++)
  set(NCC /usr/bin/gcc)
  set(NCFLAGS "-Wall -Wextra -Werror -Wno-error=extern-c-compat -Wno-error=inconsistent-missing-override -Wno-error=nullability-completeness -Wno-error=sign-compare -Wno-error=unknown-warning-option")
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
      "-Wall -Wextra -Werror -Wno-error=unknown-pragmas -lstdc++ -lstdc++fs"
      "-Wl,--no-as-needed -no-pie -ldl"
      "-I/usr/include/ -I/usr/local/include -L/usr/local/lib -L/usr/lib")

    # in my testing with aggregation func, tcmalloc performs better than jemalloc
    # turn it on as default for now
    if(NOT PPROF)
      set(PPROF 2)
    endif()

    # add a build option to enable native optimization
    if(NAT STREQUAL "1")
      set(NAT "-march=native")
    else()
      set(NAT "")
    endif()

    # TODO(cao) - fix this build option
    # enable LTO on build option
    if(LTO STREQUAL "1")
      set(LTO "-flto")
    else()
      set(LTO "")
    endif()

    # by default - do not stripe symbols in release, only when specified to reduce binary size
    if(SYM STREQUAL "1")
      set(SYM "-s")
    else()
      set(SYM "")
    endif()

    # turn on GDB debug info if using gdb
    if(GDB STREQUAL "1")
      set(GDB "-g")
    else()
      set(GDB "")
    endif()

    # build flag -DGPROF
    if (GPROF STREQUAL "1")
      # using gprof  
      list(APPEND LNCFLAGS 
        "-pg -DGPROF")
    # build flag -DPPROF -> gperftools
    elseif(PPROF STREQUAL "1")
      # or using gperftools, ask each target to depends on perftools_library for linking
      # unwind is needed by gperftools
      list(APPEND LNCFLAGS
      "-lunwind"
      "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free"
      "-DPPROF")
    elseif(PPROF STREQUAL "2")
      # or using gperftools, ask each target to depends on perftools_library for linking
      # unwind is needed by gperftools
      list(APPEND LNCFLAGS
      "-lunwind"
      "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
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
    # for libfmt, build with header only, we can append this at the tail "-DFMT_HEADER_ONLY=1"
    # About compability, if we specify -march=native, gcc will optimize code for current build machine
    # we can use this command to figure out what the real vaue of arch is `gcc -march=native -Q --help=target|grep march`
    # On my daily dev machine, it gives me this value: skylake-avx512
    # In another machine, I found it as "haswell", and this may lead crash with `illegal instruction` failure.
    # So to make sure the published image works more broadly, we may want to remove this `-march=native`, or specify the target value
    set(NCFLAGS_RELEASE "-Ofast ${NAT} ${SYM} ${LTO} ${GDB}")
  endif()
endif()

# by default using clang on mac and gcc on linux
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
  add_link_options(-L${OPT_DIR}/icu4c/lib)
else()
  set(DL_EXT "so")
  set(CELLAR_ROOT /home/linuxbrew/.linuxbrew/Cellar)
endif()

# TODO(cao) - move to jemalloc
# ref https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919/

########################## package managed ##########################
# find boost
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost MODULE
  COMPONENTS
    context
    filesystem
    program_options
    regex
    system
    thread
  REQUIRED)
if(Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
endif()

# find glog
# target: glog::glog
# find_package(glog CONFIG REQUIRED)

# find gflags
# target: gflags_static)
# find_package(gflags CONFIG REQUIRED)

# find gtest
# target: GTest::gmock GTest::gtest GTest::gmock_main GTest::gtest_main
# find_package(GTest CONFIG REQUIRED)

# find folly
# target: Folly::folly Folly::folly_deps
# find_package(folly CONFIG REQUIRED)

# find lz4
# target lz4::lz4
# find_package(lz4 CONFIG REQUIRED)

# find zstd
# target: zstd::libzstd_static
# find_package(zstd CONFIG REQUIRED)

# find snappy
# target: Snappy::snappy
# find_package(Snappy CONFIG REQUIRED)

# set up the needed tool from tool chain
# triplet: VCPKG_TARGET_TRIPLET
set(VCPKG_ROOT ${HOME_DIR}/vcpkg)
set(VCPKG_TOOLS ${VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/tools)
# protoc compiler
# set(PROTO_COMPILER ${VCPKG_TOOLS}/protobuf/protoc)
# flatbuffers compiler
# set(FLATBUFFERS_COMPILER ${VCPKG_TOOLS}/flatbuffers/flatc)
# grpc plugins: cpp plugin, node plugin 
# set(GRPC_CPP_PLUGIN ${VCPKG_TOOLS}/grpc/grpc_cpp_plugin)
# set(GRPC_NODE_PLUGIN ${VCPKG_TOOLS}/grpc/grpc_node_plugin)

########################## Self Maintained ##########################
# include xxhash
include(ext/Xxh_Ext.cmake)

# add dependency of pgquery
include(ext/PGQuery_Ext.cmake)

# add dependency of compression
include(ext/Compression_Ext.cmake)

# include date
include(ext/Date_Ext.cmake)

# add dependency of compression
include(ext/Level_Ext.cmake)

# add a dependency of glog
include(ext/Gflags_Ext.cmake)

# add a dependency of glog
include(ext/Glog_Ext.cmake)

# double conversion
include(ext/Doubleconversion_Ext.cmake)

# lib event
include(ext/Libevent_Ext.cmake)

# include highway for SIMD
# include(ext/Highway_Ext.cmake)

# add a dependency of fmt 
include(ext/Fmt_Ext.cmake)

# add a dependency of bf
include(ext/Bloom_Ext.cmake)

# add a dependency of roaring
include(ext/Roaring_Ext.cmake)

# add a dependency of re2
include(ext/RE2_Ext.cmake)

# add a basic gtest dependency
include(ext/Gtest_Ext.cmake)

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})
include_directories(include ${GMOCK_INCLUDE_DIRS})

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

# include AZURE SDK project
include(ext/Azure_Ext.cmake)

# include AWS external project
include(ext/Gcp_Ext.cmake)

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

# include yaml
include(ext/Yaml_Ext.cmake)

# include uri parser
include(ext/Uri_Ext.cmake)

# include msgpack - tuples serde
include(ext/Msgpack_Ext.cmake)

# include perf tools
include(ext/Perf_Ext.cmake)

# include kafka library
include(ext/Kafka_Ext.cmake)

# include JS execution engine
include(ext/JS_Ext.cmake)

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

# build meta module
include(src/execution/Execution.cmake)

# build data ingestion
include(src/ingest/Ingest.cmake)

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

# ensure test folder are all copied into build folder to test resources
# configure_file(${NEBULA_ROOT}/test ${CMAKE_CURRENT_BINARY_DIR}/test COPYONLY)
file(COPY ${NEBULA_ROOT}/test DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
