set(NEBULA_API NApi)

# build nebula.api library
# target_include_directories(${NEBULA_API} INTERFACE src/api)
add_library(${NEBULA_API} STATIC 
    ${NEBULA_SRC}/api/dsl/Base.cpp
    ${NEBULA_SRC}/api/dsl/Dsl.cpp
    ${NEBULA_SRC}/api/dsl/Expressions.cpp
    ${NEBULA_SRC}/api/dsl/Serde.cpp
    ${NEBULA_SRC}/api/udf/Avg.cpp
    ${NEBULA_SRC}/api/udf/Like.cpp
    ${NEBULA_SRC}/api/udf/Sum.cpp)
target_link_libraries(${NEBULA_API}
    PUBLIC ${NEBULA_TYPE}
    PUBLIC ${NEBULA_COMMON}
    PUBLIC ${NEBULA_SURFACE}
    PUBLIC ${NEBULA_META}
    PUBLIC ${NEBULA_EXEC}
    PUBLIC ${FOLLY_LIBRARY}
    PUBLIC ${MSGPACK_LIBRARY}
    PUBLIC ${XXH_LIBRARY}
    PUBLIC ${JSON_LIBRARY}
    PUBLIC ${FMT_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${ROARING_LIBRARY})

# build test binary
add_executable(ApiTests 
    ${NEBULA_SRC}/api/test/Test.cpp
    ${NEBULA_SRC}/api/test/TestApi.cpp
    ${NEBULA_SRC}/api/test/TestExpressions.cpp
    ${NEBULA_SRC}/api/test/TestQuery.cpp
    ${NEBULA_SRC}/api/test/TestUdf.cpp)

target_link_libraries(ApiTests 
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${NEBULA_API}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${GMOCK_LIBRARY}
    PRIVATE ${XXH_LIBRARY}
    PRIVATE ${JSON_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})

if(APPLE)
    target_compile_options(ApiTests PRIVATE -Wno-error=unknown-warning-option)
endif()

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(ApiTests TEST_LIST ALL
    DISCOVERY_TIMEOUT 10)