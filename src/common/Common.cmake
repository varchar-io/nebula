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
    ${NEBULA_SRC}/common/test/TestDistAlgo.cpp
    ${NEBULA_SRC}/common/test/TestExts.cpp
    ${NEBULA_SRC}/common/test/TestHash.cpp
    ${NEBULA_SRC}/common/test/TestIpAddr.cpp
    ${NEBULA_SRC}/common/test/TestJs.cpp
    ${NEBULA_SRC}/common/test/TestMetaDB.cpp
    ${NEBULA_SRC}/common/test/TestSimd.cpp
    ${NEBULA_SRC}/common/test/TestSlice.cpp
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
    PRIVATE ${JSON_LIBRARY}
    PRIVATE ${BF_LIBRARY}
    PRIVATE ${CF_LIBRARY}
    PRIVATE ${YAML_LIBRARY}
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${QJS_LIBRARY}
    PRIVATE ${LEVELDB_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(CommonTests TEST_LIST ALL)
