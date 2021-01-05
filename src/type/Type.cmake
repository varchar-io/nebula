set(NEBULA_TYPE NType)

# include folders from types
# target_include_directories(${NEBULA_TYPE} INTERFACE src/type)
add_library(${NEBULA_TYPE} STATIC 
    ${NEBULA_SRC}/type/Serde.cpp)
target_link_libraries(${NEBULA_TYPE}
    PUBLIC ${NEBULA_COMMON})

add_executable(TypeTests 
    ${NEBULA_SRC}/type/test/TestType.cpp
    ${NEBULA_SRC}/type/test/TestMisc.cpp)

target_link_libraries(TypeTests 
    PRIVATE ${NEBULA_TYPE}
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TypeTests TEST_LIST ALL
    DISCOVERY_TIMEOUT 10)
