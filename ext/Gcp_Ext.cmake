find_package(Threads REQUIRED)

include(ExternalProject)

# install abseil (required >= c++ 11)
SET(ABSL_OPTS
    -DBUILD_TESTING=OFF
    -DCMAKE_CXX_STANDARD=17)
ExternalProject_Add(absl
    PREFIX absl
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20200923.2
    CMAKE_ARGS ${ABSL_OPTS}
    UPDATE_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# include nlohmann json
ExternalProject_Add(nlohmann
    PREFIX nlohmann
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.9.1
    UPDATE_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

SET(CRC32C_OPTS
    -DCRC32C_BUILD_TESTS=OFF
    -DCRC32C_BUILD_BENCHMARKS=OFF)
ExternalProject_Add(crc32c
    PREFIX crc32c
    GIT_REPOSITORY https://github.com/google/crc32c.git
    GIT_TAG 1.1.1
    CMAKE_ARGS ${CRC32C_OPTS}
    UPDATE_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

SET(GCP_OPTS
    -DBUILD_TESTING=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_BIGQUERY=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_BIGTABLE=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_SPANNER=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_FIRESTORE=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_PUBSUB=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_IAM=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_GENERATOR=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_GRPC=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE=storage
    -DGOOGLE_CLOUD_CPP_STORAGE_ENABLE_GRPC=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_CXX_EXCEPTIONS=OFF
    -DGOOGLE_CLOUD_CPP_ENABLE_MACOS_OPENSSL_CHECK=OFF)

# can not use native one as Apple closed its usage
# can be installed by brew
if(APPLE)
    SET(GCP_OPTS ${GCP_OPTS} "-DOPENSSL_ROOT_DIR=/usr/local/opt/openssl")
endif()

ExternalProject_Add(gcp
    PREFIX gcp
    GIT_REPOSITORY https://github.com/googleapis/google-cloud-cpp.git
    GIT_TAG v1.21.0
    # SOURCE_SUBDIR google/cloud/storage
    CMAKE_ARGS ${GCP_OPTS}
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# gcp depends on absl
add_dependencies(gcp absl nlohmann crc32c)

# get source dir after download step
#   ExternalProject_Get_Property(highway SOURCE_DIR)
#   ExternalProject_Get_Property(highway BINARY_DIR)
#   set(HWY_INCLUDE_DIRS ${SOURCE_DIR})
#   file(MAKE_DIRECTORY ${HWY_INCLUDE_DIRS})
#   set(HWY_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}hwy.a)
#   set(HWY_LIBRARY libhwy)
#   add_library(${HWY_LIBRARY} UNKNOWN IMPORTED)
#   set_target_properties(${HWY_LIBRARY} PROPERTIES
#       "IMPORTED_LOCATION" "${HWY_LIBRARY_PATH}"
#       "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
#       "INTERFACE_INCLUDE_DIRECTORIES" "${HWY_INCLUDE_DIRS}")
#   add_dependencies(${HWY_LIBRARY} highway)
