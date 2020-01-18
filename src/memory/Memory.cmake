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
    ${NEBULA_SRC}/memory/serde/TypeDataFactory.cpp
    ${NEBULA_SRC}/memory/serde/TypeMetadata.cpp)
target_link_libraries(${NEBULA_MEMORY}
    PUBLIC ${NEBULA_COMMON}
    PUBLIC ${NEBULA_TYPE}
    PUBLIC ${NEBULA_SURFACE}
    PUBLIC ${NEBULA_META}
    PUBLIC ${FOLLY_LIBRARY})

# build test binary
add_executable(MemoryTests 
    ${NEBULA_SRC}/memory/test/TestArrow.cpp
    ${NEBULA_SRC}/memory/test/TestBatch.cpp
    ${NEBULA_SRC}/memory/test/TestEncoder.cpp
    ${NEBULA_SRC}/memory/test/TestFlatBuffer.cpp
    ${NEBULA_SRC}/memory/test/TestFlatRow.cpp
    ${NEBULA_SRC}/memory/test/TestHistogram.cpp)

target_link_libraries(MemoryTests
    PRIVATE ${NEBULA_MEMORY}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${XXH_LIBRARY}
    PRIVATE ${ROARING_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(MemoryTests TEST_LIST ALL)
