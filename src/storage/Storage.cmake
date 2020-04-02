set(NEBULA_STORAGE NStorage)

# build nebula.api library
# target_include_directories(${NEBULA_META} INTERFACE src/meta)
add_library(${NEBULA_STORAGE} STATIC 
    ${NEBULA_SRC}/storage/CsvReader.cpp
    ${NEBULA_SRC}/storage/NFS.cpp
    ${NEBULA_SRC}/storage/ParquetReader.cpp
    ${NEBULA_SRC}/storage/aws/S3.cpp
    ${NEBULA_SRC}/storage/kafka/KafkaProvider.cpp
    ${NEBULA_SRC}/storage/kafka/KafkaReader.cpp
    ${NEBULA_SRC}/storage/kafka/KafkaTopic.cpp
    ${NEBULA_SRC}/storage/local/File.cpp)
target_link_libraries(${NEBULA_STORAGE}
    PUBLIC ${NEBULA_COMMON}
    PUBLIC ${NEBULA_SURFACE}
    PUBLIC ${URIP_LIBRARY}
    PUBLIC ${PARQUET_LIBRARY}
    PUBLIC ${ARROW_LIBRARY}
    PUBLIC ${AWS_LIBRARY}
    PUBLIC ${KAFKA_LIBRARY}
    PUBLIC ${CURL_LIBRARY}
    PUBLIC ${THRIFT_LIBRARY})

#build test binary
add_executable(StorageTests
    ${NEBULA_SRC}/storage/test/TestJsonReader.cpp
    ${NEBULA_SRC}/storage/test/TestKafka.cpp
    ${NEBULA_SRC}/storage/test/TestParquet.cpp
    ${NEBULA_SRC}/storage/test/TestStorage.cpp)

# Why we choose PRIVATE rather than PUBLIC
# The main reason is deal with link order, each dependency may need adjust position
# to allow each other find correct reference
# GCC looks symbols from a lib after itself?? That's why we need to place aws-core after aws-s3
# CLANG doesn't have this issue
target_link_libraries(StorageTests  
    PRIVATE ${NEBULA_STORAGE}
    PRIVATE ${NEBULA_TYPE}    
    PRIVATE ${NEBULA_MEMORY}    
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${URIP_LIBRARY}
    PRIVATE ${FOLLY_LIBRARY}   
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${BOOST_REGEX_LIBRARY}
    PRIVATE ${BOOST_FS_LIBRARY}
    PRIVATE ${THRIFT_LIBRARY}
    PRIVATE ${ZLIB_LIBRARY}
    PRIVATE ${SNAPPY_LIBRARY}
    PRIVATE ${XXH_LIBRARY}
    PRIVATE ${AWS_COMMON_LIBRARY}
    PRIVATE ${AWS_S3_LIBRARY}
    PRIVATE ${AWS_CORE_LIBRARY}
    PRIVATE ${KAFKA_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(StorageTests TEST_LIST ALL)