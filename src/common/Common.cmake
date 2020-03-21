set(NEBULA_COMMON NCommon)

# build nebula.common library
# target_include_directories(${NEBULA_COMMON} INTERFACE src/common)
add_library(${NEBULA_COMMON} STATIC 
    ${NEBULA_SRC}/common/Errors.cpp 
    ${NEBULA_SRC}/common/Memory.cpp
    ${NEBULA_SRC}/common/Int128.cpp)
target_link_libraries(${NEBULA_COMMON}
    PUBLIC ${FMT_LIBRARY}
    PUBLIC ${XXH_LIBRARY}
    PUBLIC ${FOLLY_LIBRARY})

# build test binary
add_executable(CommonTests 
    ${NEBULA_SRC}/common/test/TestCommon.cpp
    ${NEBULA_SRC}/common/test/TestCompression.cpp
    ${NEBULA_SRC}/common/test/TestExts.cpp
    ${NEBULA_SRC}/common/test/TestSimd.cpp
    ${NEBULA_SRC}/common/test/TestStatsAlgo.cpp)

target_link_libraries(CommonTests 
    PRIVATE ${NEBULA_COMMON}    
    PRIVATE ${OMM_LIBRARY}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${MSGPACK_LIBRARY}
    PRIVATE ${ROARING_LIBRARY}
    PRIVATE ${HWY_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${BF_LIBRARY}
    PRIVATE ${CF_LIBRARY}
    PRIVATE ${YAML_LIBRARY}
    PRIVATE ${FOLLY_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(CommonTests TEST_LIST ALL)
