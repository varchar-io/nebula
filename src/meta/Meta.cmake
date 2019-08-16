set(NEBULA_META NMeta)

# build nebula.api library
# target_include_directories(${NEBULA_META} INTERFACE src/meta)
add_library(${NEBULA_META} STATIC 
    ${NEBULA_SRC}/meta/Table.cpp
    ${NEBULA_SRC}/meta/TestTable.cpp
    ${NEBULA_SRC}/meta/ClusterInfo.cpp)
target_link_libraries(${NEBULA_META}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_TYPE}
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${YAML_LIBRARY}
    PRIVATE ${XXH_LIBRARY})

# include its own root directory for searching headers
# set(NMETA_INCLUDE_DIRS ${NEBULA_SRC}/meta)
# include_directories(include ${NMETA_INCLUDE_DIRS})

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
add_executable(MetaTests
    ${NEBULA_SRC}/meta/test/TestMeta.cpp)

target_link_libraries(MetaTests 
    PRIVATE ${NEBULA_META}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${XXH_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(MetaTests TEST_LIST ALL)
