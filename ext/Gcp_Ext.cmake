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

# TODO - trying to install these dependencies in build folder
# and let GCP use find_package to find them.
# so that we don't need to `sudo make`
# allow find_package to find it in build folder
# SET(absl_ROOT ${${CMAKE_CURRENT_BINARY_DIR}/absl})

# include nlohmann json
ExternalProject_Add(nlohmann
    PREFIX nlohmann
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.9.1
    UPDATE_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# allow find_package to find it in build folder
# ExternalProject_Get_Property(nlohmann BINARY_DIR)
# SET(nlohmann_json_ROOT ${BINARY_DIR})

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

# allow find_package to find it in build folder
# ExternalProject_Get_Property(crc32c BINARY_DIR)
# SET(Crc32c_ROOT ${BINARY_DIR})

# specify each package root for GCP to find if install correctly
# -DCMAKE_POLICY_DEFAULT_CMP0074=NEW
# -Dabsl_ROOT=${absl_ROOT}
# -Dnlohmann_json_ROOT=${nlohmann_json_ROOT}
# -DCrc32c_ROOT=${Crc32c_ROOT}
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
ExternalProject_Get_Property(gcp SOURCE_DIR)
ExternalProject_Get_Property(gcp BINARY_DIR)
set(GCP_INCLUDE_DIRS ${SOURCE_DIR})

# gcp common lib
set(GCP_COMMON_LIB ${BINARY_DIR}/google/cloud/libgoogle_cloud_cpp_common.a)
set(GCP_COMM_LIBRARY libgcpcomm)
add_library(${GCP_COMM_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GCP_COMM_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GCP_COMMON_LIB}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GCP_INCLUDE_DIRS}")

# gcs lib
set(GCS_LIB ${BINARY_DIR}/google/cloud/storage/libstorage_client.a)
set(GCS_LIBRARY libgcs)
add_library(${GCS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GCS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GCS_LIB}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GCP_INCLUDE_DIRS}")

# declare the build dependency and link dependency
add_dependencies(${GCS_LIBRARY} gcp)
target_link_libraries(${GCS_LIBRARY}
    INTERFACE ${GCP_COMM_LIBRARY})
