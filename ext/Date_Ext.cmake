# Before this library goes to future C++ standard.
# We use it for much more complex time parsing.
# https://github.com/HowardHinnant/date.git
find_package(Threads REQUIRED)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
ExternalProject_Add(date
  PREFIX date
  GIT_REPOSITORY https://github.com/HowardHinnant/date.git
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(date SOURCE_DIR)

set(DATE_LIBRARY datelib)
set(DATE_INCLUDE_DIR ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${DATE_INCLUDE_DIR})
add_library(${DATE_LIBRARY} INTERFACE)
set_target_properties(${DATE_LIBRARY} PROPERTIES
    "INTERFACE_INCLUDE_DIRECTORIES" "${DATE_INCLUDE_DIR}")
add_dependencies(${DATE_LIBRARY} date)
