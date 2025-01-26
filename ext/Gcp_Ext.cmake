find_package(Threads REQUIRED)
find_package(absl REQUIRED)
find_package(Crc32c REQUIRED)

include(ExternalProject)

SET(GCP_OPTS
    -DCMAKE_CXX_STANDARD:STRING=17
    -DBUILD_TESTING=OFF
    -DGOOGLE_CLOUD_CPP_WITH_MOCKS=OFF
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
    -DGOOGLE_CLOUD_CPP_ENABLE_MACOS_OPENSSL_CHECK=OFF
    -DCMAKE_CXX_FLAGS=-Wno-error=deprecated-declarations)

# can not use native one as Apple closed its usage
# can be installed by brew
if(APPLE)
    SET(GCP_OPTS ${GCP_OPTS} "-DOPENSSL_ROOT_DIR=${OPT_DIR}/openssl")
endif()

ExternalProject_Add(gcp
    PREFIX gcp
    GIT_REPOSITORY https://github.com/googleapis/google-cloud-cpp.git
    GIT_TAG v2.33.0
    # SOURCE_SUBDIR google/cloud/storage
    CMAKE_ARGS ${GCP_OPTS}
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

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

# gcp rest internal lib
set(GCP_RESTINT_LIB ${BINARY_DIR}/google/cloud/libgoogle_cloud_cpp_rest_internal.a)
set(GCP_RESTINT_LIBRARY libgcprestint)
add_library(${GCP_RESTINT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GCP_RESTINT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GCP_RESTINT_LIB}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GCP_INCLUDE_DIRS}")

# find abseil since we already installed it
# add_dependencies(${GCP_COMM_LIBRARY} absl Crc32c)
target_link_libraries(${GCP_COMM_LIBRARY}
    INTERFACE absl::strings
    INTERFACE absl::time
    INTERFACE absl::bad_optional_access
    INTERFACE absl::str_format_internal
    INTERFACE absl::crc32c
    INTERFACE Crc32c::crc32c
    INTERFACE ${CURL_LIBRARY}
    INTERFACE ${OPENSSL_LIBRARY}
    INTERFACE ${CRYPTO_LIBRARY})

# gcs lib
set(GCS_LIB ${BINARY_DIR}/google/cloud/storage/libgoogle_cloud_cpp_storage.a)
set(GCS_LIBRARY libgcs)
add_library(${GCS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GCS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GCS_LIB}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GCP_INCLUDE_DIRS}")

# declare the build dependency and link dependency
add_dependencies(${GCS_LIBRARY} gcp)
target_link_libraries(${GCS_LIBRARY}
    INTERFACE ${GCP_COMM_LIBRARY}
    INTERFACE ${GCP_RESTINT_LIBRARY})
