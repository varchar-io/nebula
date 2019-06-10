set(NEBULA_EXEC NExec)

# build nebula.exec library
# target_include_directories(${NEBULA_EXEC} INTERFACE src/execution)
add_library(${NEBULA_EXEC} STATIC 
    ${NEBULA_SRC}/execution/core/BlockExecutor.cpp    
    ${NEBULA_SRC}/execution/core/ComputedRow.cpp    
    ${NEBULA_SRC}/execution/core/NodeClient.cpp    
    ${NEBULA_SRC}/execution/core/NodeExecutor.cpp    
    ${NEBULA_SRC}/execution/core/ServerExecutor.cpp    
    ${NEBULA_SRC}/execution/eval/EvalContext.cpp    
    ${NEBULA_SRC}/execution/io/BlockLoader.cpp
    ${NEBULA_SRC}/execution/io/trends/Comments.cpp
    ${NEBULA_SRC}/execution/io/trends/Pins.cpp
    ${NEBULA_SRC}/execution/io/trends/Trends.cpp
    ${NEBULA_SRC}/execution/op/Operator.cpp
    ${NEBULA_SRC}/execution/BlockManager.cpp
    ${NEBULA_SRC}/execution/ExecutionPlan.cpp)

target_link_libraries(${NEBULA_EXEC}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_META}
    PRIVATE ${NEBULA_MEMORY}
    PRIVATE ${NEBULA_SURFACE}
    PRIVATE ${NEBULA_STORAGE}
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${ROARING_LIBRARY}
    PRIVATE ${JSON_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})

# include its own root directory for searching headers
# set(NEXEC_INCLUDE_DIRS ${NEBULA_SRC}/execution)
# include_directories(include ${NEXEC_INCLUDE_DIRS})

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
add_executable(ExecTests
    ${NEBULA_SRC}/execution/test/TestExec.cpp
    ${NEBULA_SRC}/execution/test/TestValueEvalTree.cpp)

target_link_libraries(ExecTests 
    PRIVATE ${NEBULA_EXEC}
    PRIVATE ${GTEST_LIBRARY}
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${GMOCK_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(ExecTests TEST_LIST ALL)