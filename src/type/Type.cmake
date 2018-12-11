set(NEBULA_TYPE NType)

# test directory macros in current cmake runtime
message(STATUS "NEBULA_ROOT : ${NEBULA_ROOT}")
message(STATUS "NEBULA_SRC : ${NEBULA_SRC}")
# message(STATUS "CMAKE_ROOT : ${CMAKE_ROOT}")
# message(STATUS "PROJECT_SOURCE_DIR : ${PROJECT_SOURCE_DIR}")
# message(STATUS "PROJECT_BINARY_DIR : ${PROJECT_BINARY_DIR}")
# message(STATUS "CMAKE_CURRENT_SOURCE_DIR : ${CMAKE_CURRENT_SOURCE_DIR}")
# message(STATUS "CMAKE_CURRENT_BINARY_DIR : ${CMAKE_CURRENT_BINARY_DIR}")
# message(STATUS "CMAKE_SOURCE_DIR : ${CMAKE_SOURCE_DIR}")
# message(STATUS "CMAKE_BINARY_DIR : ${CMAKE_BINARY_DIR}")
# message(STATUS "CMAKE_CURRENT_LIST_DIR : ${CMAKE_CURRENT_LIST_DIR}")

# ask for gflags
include_directories(include ${GFLAGS_INCLUDE_DIRS})

# ask for glog
include_directories(include ${GLOG_INCLUDE_DIRS})

# it depends on fmt
include_directories(include ${FMT_INCLUDE_DIRS})

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})

# include folders from types
add_library(${NEBULA_TYPE} INTERFACE)
target_sources(${NEBULA_TYPE} INTERFACE)
target_include_directories(${NEBULA_TYPE} INTERFACE src/type)

add_executable(TypeTests ${NEBULA_SRC}/type/test/TestType.cpp)
target_link_libraries(TypeTests 
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PUBLIC ${NEBULA_COMMON}
    PRIVATE ${NEBULA_TYPE})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TypeTests TEST_LIST ALL)
