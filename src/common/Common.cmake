set(NEBULA_COMMON NCommon)

# build nebula.common library
# target_include_directories(${NEBULA_COMMON} INTERFACE src/common)
add_library(${NEBULA_COMMON} STATIC 
    ${NEBULA_SRC}/common/Errors.cpp 
    ${NEBULA_SRC}/common/Memory.cpp)
target_link_libraries(${NEBULA_COMMON}
    PRIVATE ${FMT_LIBRARY})

# include itself for headers in different folders
# set(NCOMMON_INCLUDE_DIRS ${NEBULA_SRC}/common)
# include_directories(include ${NCOMMON_INCLUDE_DIRS})

# ask for gflags
include_directories(include ${GFLAGS_INCLUDE_DIRS})

# ask for glog
include_directories(include ${GLOG_INCLUDE_DIRS})

# it depends on fmt
include_directories(include ${FMT_INCLUDE_DIRS})

# it depends on roaring
include_directories(include ${ROARING_INCLUDE_DIRS})

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})

# build test binary
add_executable(CommonTests 
    ${NEBULA_SRC}/common/test/TestCommon.cpp
    ${NEBULA_SRC}/common/test/TestExts.cpp)

target_link_libraries(CommonTests 
    PRIVATE ${NEBULA_COMMON}    
    PRIVATE ${OMM_LIBRARY}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${ROARING_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${BF_LIBRARY}
    PRIVATE ${CF_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(CommonTests TEST_LIST ALL)