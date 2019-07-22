set(NEBULA_MEMORY NMemory)

# build nebula.memory library - data representation in memory
# target_include_directories(${NEBULA_MEMORY} INTERFACE src/memory)
add_library(${NEBULA_MEMORY} STATIC 
    ${NEBULA_SRC}/memory/FlatRow.cpp
    ${NEBULA_SRC}/memory/DataNode.cpp
    ${NEBULA_SRC}/memory/Batch.cpp
    ${NEBULA_SRC}/memory/Accessor.cpp
    ${NEBULA_SRC}/memory/encode/RleEncoder.cpp
    ${NEBULA_SRC}/memory/encode/RleDecoder.cpp
    ${NEBULA_SRC}/memory/keyed/FlatBuffer.cpp
    ${NEBULA_SRC}/memory/keyed/HashFlat.cpp
    ${NEBULA_SRC}/memory/serde/TypeData.cpp
    ${NEBULA_SRC}/memory/serde/TypeDataFactory.cpp)
target_link_libraries(${NEBULA_MEMORY}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_TYPE}
    PRIVATE ${NEBULA_SURFACE}
    PRIVATE ${NEBULA_META}
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})

# include itself for headers in different folders
# set(NMEMORY_INCLUDE_DIRS ${NEBULA_SRC}/memory)
# include_directories(include ${NMEMORY_INCLUDE_DIRS})

# ask for gflags
include_directories(include ${GFLAGS_INCLUDE_DIRS})

# ask for glog
include_directories(include ${GLOG_INCLUDE_DIRS})

# it depends on fmt
include_directories(include ${FMT_INCLUDE_DIRS})

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})

# build test binary
add_executable(MemoryTests 
    ${NEBULA_SRC}/memory/test/TestArrow.cpp
    ${NEBULA_SRC}/memory/test/TestBatch.cpp
    ${NEBULA_SRC}/memory/test/TestEncoder.cpp
    ${NEBULA_SRC}/memory/test/TestFlatBuffer.cpp)

target_link_libraries(MemoryTests
    PRIVATE ${NEBULA_MEMORY}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${ROARING_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(MemoryTests TEST_LIST ALL)