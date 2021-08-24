find_package(Threads REQUIRED)

include(ExternalProject)

# install abseil (required >= c++ 11)
# Note: use 11 rather than 17 so that we can use absl::string_view rather than std::string_view
# because GCP common references absl::string_view directly
SET(ABSL_OPTS
    -DBUILD_TESTING=OFF
    -DBUILD_SHARED_LIBS=OFF
    -DABSL_USES_STD_STRING_VIEW=ON
    -DABSL_USES_STD_OPTIONAL=ON
    -DCMAKE_CXX_STANDARD=11)
ExternalProject_Add(absl
    PREFIX absl
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20200923.3
    CMAKE_ARGS ${ABSL_OPTS}
    INSTALL_DIR ${NEBULA_INSTALL}
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)