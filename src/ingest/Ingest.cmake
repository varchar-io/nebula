# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(NEBULA_INGEST NIngest)

# build nebula.ingest library
add_library(${NEBULA_INGEST} STATIC 
    ${NEBULA_SRC}/ingest/IngestSpec.cpp
    ${NEBULA_SRC}/ingest/SpecRepo.cpp)
target_link_libraries(${NEBULA_INGEST}
    PUBLIC ${NEBULA_COMMON}
    PUBLIC ${NEBULA_TYPE}
    PUBLIC ${NEBULA_META}
    PUBLIC ${NEBULA_STORAGE}
    PUBLIC ${NEBULA_MEMORY}
    PUBLIC ${NEBULA_EXEC}
    PUBLIC ${URIP_LIBRARY}
    PUBLIC ${FMT_LIBRARY}
    PUBLIC ${YAML_LIBRARY}
    PUBLIC ${XXH_LIBRARY}
    PUBLIC ${AWS_LIBRARY}
    PUBLIC ${PERF_LIBRARY})

# build test binary
add_executable(IngestTests
    ${NEBULA_SRC}/ingest/test/TestIngestion.cpp)

target_link_libraries(IngestTests 
    PRIVATE ${NEBULA_INGEST}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${URIP_LIBRARY}
    PRIVATE ${FOLLY_LIBRARY}   
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${Boost_regex_LIBRARY}
    PRIVATE ${THRIFT_LIBRARY}
    PRIVATE ${ZLIB_LIBRARY}
    PRIVATE ${SNAPPY_LIBRARY}
    PRIVATE ${XXH_LIBRARY}
    PRIVATE ${LIBPG_QUERY_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(IngestTests TEST_LIST ALL)
