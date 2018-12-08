set(NEBULA_TYPE NType)

# add_library(${NEBULA_TYPE}  Type.cpp)

# ask for gflags
include_directories(include ${GFLAGS_INCLUDE_DIRS})

# ask for glog
include_directories(include ${GLOG_INCLUDE_DIRS})

# it depends on fmt
include_directories(include ${FMT_INCLUDE_DIRS})

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})
add_executable(TypeTests src/type/test/TestType.cpp)
target_link_libraries(TypeTests 
    ${GTEST_LIBRARY} 
    ${GTEST_MAIN_LIBRARY} 
    ${FMT_LIBRARY}
    ${GFLAGS_LIBRARY}
    ${GLOG_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TypeTests TEST_LIST ALL)
