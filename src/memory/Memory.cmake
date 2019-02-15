set(NEBULA_MEMORY NMemory)

# build nebula.memory library - data representation in memory
add_library(${NEBULA_MEMORY} STATIC 
    ${NEBULA_SRC}/memory/FlatRow.cpp
    ${NEBULA_SRC}/memory/DataNode.cpp
    ${NEBULA_SRC}/memory/Batch.cpp
    ${NEBULA_SRC}/memory/serde/TypeDataFactory.cpp)
target_include_directories(${NEBULA_MEMORY} INTERFACE src/memory)
target_link_libraries(${NEBULA_MEMORY}
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_TYPE}
    PRIVATE ${NEBULA_SURFACE})

# ask for gflags
include_directories(include ${GFLAGS_INCLUDE_DIRS})

# ask for glog
include_directories(include ${GLOG_INCLUDE_DIRS})

# it depends on fmt
include_directories(include ${FMT_INCLUDE_DIRS})

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})

# build test binary
add_executable(MemoryTests ${NEBULA_SRC}/memory/test/TestBatch.cpp)
target_link_libraries(MemoryTests 
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_TYPE}
    PRIVATE ${NEBULA_SURFACE}
    PRIVATE ${NEBULA_MEMORY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(MemoryTests TEST_LIST ALL)