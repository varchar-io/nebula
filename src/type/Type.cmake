set(NEBULA_TYPE NType)

# add_library(${NEBULA_TYPE}  Type.cpp)

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})
add_executable(TypeTests src/type/test/TestType.cpp)
target_link_libraries(TypeTests ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})
include(GoogleTest)
gtest_discover_tests(TypeTests TEST_LIST ALL)
