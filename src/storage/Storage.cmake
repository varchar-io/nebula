set(NEBULA_STORAGE NStorage)

# build nebula.api library
# target_include_directories(${NEBULA_META} INTERFACE src/meta)
add_library(${NEBULA_STORAGE} STATIC 
    ${NEBULA_SRC}/storage/CsvReader.cpp
    ${NEBULA_SRC}/storage/aws/S3.cpp)
target_link_libraries(${NEBULA_STORAGE}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_SURFACE}
    PRIVATE ${AWS_CORE_LIBRARY}
    PRIVATE ${AWS_S3_LIBRARY}
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${FMT_LIBRARY})

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

#build test binary
add_executable(StorageTests
    ${NEBULA_SRC}/storage/test/TestStorage.cpp)

target_link_libraries(StorageTests 
    PRIVATE ${NEBULA_STORAGE}    
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${ROARING_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(StorageTests TEST_LIST ALL)