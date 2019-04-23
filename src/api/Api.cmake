set(NEBULA_API NApi)

# build nebula.api library
# target_include_directories(${NEBULA_API} INTERFACE src/api)
add_library(${NEBULA_API} STATIC 
    ${NEBULA_SRC}/api/dsl/Base.cpp
    ${NEBULA_SRC}/api/dsl/Dsl.cpp
    ${NEBULA_SRC}/api/dsl/Expressions.cpp
    ${NEBULA_SRC}/api/udf/Count.cpp
    ${NEBULA_SRC}/api/udf/Like.cpp
    ${NEBULA_SRC}/api/udf/Sum.cpp
    ${NEBULA_SRC}/api/udf/UDFFactory.cpp)
target_link_libraries(${NEBULA_API}
    PRIVATE ${NEBULA_TYPE}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_SURFACE}
    PRIVATE ${NEBULA_META}
    PRIVATE ${NEBULA_EXEC}
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${ROARING_LIBRARY})

# include its own root directory for searching headers
# set(NAPI_INCLUDE_DIRS ${NEBULA_SRC}/api)
# include_directories(include ${NAPI_INCLUDE_DIRS})

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

# include gmock headers
include_directories(include ${GMOCK_INCLUDE_DIRS})

# build test binary
add_executable(ApiTests 
    ${NEBULA_SRC}/api/test/TestApi.cpp
    ${NEBULA_SRC}/api/test/TestExpressions.cpp
    ${NEBULA_SRC}/api/test/TestQuery.cpp
    ${NEBULA_SRC}/api/test/TestUdf.cpp)

target_link_libraries(ApiTests 
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${NEBULA_API}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${GMOCK_LIBRARY})

target_compile_options(ApiTests PRIVATE -Wno-error=unknown-warning-option)

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(ApiTests TEST_LIST ALL)