set(NEBULA_SURFACE NSurface)

# build nebula.surface library - data exchange surface API
# target_include_directories(${NEBULA_SURFACE} INTERFACE src/surface)
add_library(${NEBULA_SURFACE} STATIC 
    ${NEBULA_SRC}/surface/MockSurface.cpp
    ${NEBULA_SRC}/surface/eval/EvalContext.cpp)
target_link_libraries(${NEBULA_SURFACE}
    PUBLIC ${NEBULA_TYPE}
    PUBLIC ${NEBULA_COMMON}
    PUBLIC ${FMT_LIBRARY})

# build test binary
add_executable(SurfaceTests 
    ${NEBULA_SRC}/surface/test/TestSurface.cpp)
target_link_libraries(SurfaceTests 
    PRIVATE ${NEBULA_COMMON}   
    PRIVATE ${NEBULA_SURFACE}    
    PRIVATE ${GTEST_LIBRARY} 
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${FMT_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(SurfaceTests TEST_LIST ALL)
