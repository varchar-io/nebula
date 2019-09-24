set(NEBULA_EXEC NExec)

# build nebula.exec library
# target_include_directories(${NEBULA_EXEC} INTERFACE src/execution)
add_library(${NEBULA_EXEC} STATIC 
    ${NEBULA_SRC}/execution/core/AggregationMerge.cpp    
    ${NEBULA_SRC}/execution/core/BlockExecutor.cpp    
    ${NEBULA_SRC}/execution/core/ComputedRow.cpp    
    ${NEBULA_SRC}/execution/core/NodeClient.cpp    
    ${NEBULA_SRC}/execution/core/NodeExecutor.cpp    
    ${NEBULA_SRC}/execution/core/ServerExecutor.cpp    
    ${NEBULA_SRC}/execution/core/TopSort.cpp
    ${NEBULA_SRC}/execution/eval/EvalContext.cpp    
    ${NEBULA_SRC}/execution/io/BlockLoader.cpp
    ${NEBULA_SRC}/execution/meta/TableService.cpp
    ${NEBULA_SRC}/execution/op/Operator.cpp
    ${NEBULA_SRC}/execution/serde/RowCursorSerde.cpp
    ${NEBULA_SRC}/execution/BlockManager.cpp
    ${NEBULA_SRC}/execution/ExecutionPlan.cpp)

target_link_libraries(${NEBULA_EXEC}
    PUBLIC ${NEBULA_COMMON}
    PUBLIC ${NEBULA_META}
    PUBLIC ${NEBULA_MEMORY}
    PUBLIC ${NEBULA_SURFACE}
    PUBLIC ${NEBULA_STORAGE}
    PUBLIC ${FOLLY_LIBRARY}
    PUBLIC ${XXH_LIBRARY}
    PUBLIC ${FMT_LIBRARY}
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${ROARING_LIBRARY}
    PUBLIC ${JSON_LIBRARY}
    PUBLIC ${OMM_LIBRARY}
    PUBLIC ${BOOST_REGEX_LIBRARY})

# build test binary
add_executable(ExecTests
    ${NEBULA_SRC}/execution/test/TestExec.cpp
    ${NEBULA_SRC}/execution/test/TestValueEvalTree.cpp)

target_link_libraries(ExecTests 
    PRIVATE ${NEBULA_EXEC}
    PRIVATE ${OMM_LIBRARY}
    PRIVATE ${XXH_LIBRARY}
    PRIVATE ${GTEST_LIBRARY}
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${GMOCK_LIBRARY}
    PRIVATE ${BOOST_REGEX_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(ExecTests TEST_LIST ALL)