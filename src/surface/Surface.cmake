set(NEBULA_SURFACE NSurface)

# build nebula.surface library - data exchange surface API
add_library(${NEBULA_SURFACE} STATIC 
    ${NEBULA_SRC}/surface/MockSurface.cpp)
target_include_directories(${NEBULA_SURFACE} INTERFACE src/surface)
target_link_libraries(${NEBULA_SURFACE}
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_TYPE})

# ask for gflags
include_directories(include ${GFLAGS_INCLUDE_DIRS})

# ask for glog
include_directories(include ${GLOG_INCLUDE_DIRS})

# it depends on fmt
include_directories(include ${FMT_INCLUDE_DIRS})

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})

# build test binary
add_executable(SurfaceTests ${NEBULA_SRC}/surface/test/TestSurface.cpp)
target_link_libraries(SurfaceTests 
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY} 
    PRIVATE ${FMT_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${NEBULA_COMMON}
    PRIVATE ${NEBULA_SURFACE})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(SurfaceTests TEST_LIST ALL)
