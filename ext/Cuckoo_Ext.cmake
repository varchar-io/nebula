# cuckoo filter impl by original researchers
find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
ExternalProject_Add(cuckoofilter
  PREFIX cuckoofilter
  GIT_REPOSITORY https://github.com/efficient/cuckoofilter.git
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(cuckoofilter SOURCE_DIR)

set(CF_LIBRARY cflib)
set(CF_SRC ${SOURCE_DIR}/src/hashutil.cc)
set_source_files_properties(${CF_SRC} PROPERTIES GENERATED TRUE)
add_library(${CF_LIBRARY} STATIC ${CF_SRC})
target_link_libraries(${CF_LIBRARY} PRIVATE ${OPENSSL_LIBRARY})
if(NOT APPLE)
    target_compile_options(${CF_LIBRARY} PRIVATE -Wno-error=implicit-fallthrough)
endif()

add_dependencies(${CF_LIBRARY} cuckoofilter)

# all cf headders are here
include_directories(include ${SOURCE_DIR}/src)
