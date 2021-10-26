find_package(Threads REQUIRED)

# AWS SDK build options 
# https://docs.microsoft.com/en-us/azure/storage/common/storage-samples-c-plus-plus
SET(AZURE_CMAKE_BUILD_OPTIONS
  -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}
  -DAZ_ALL_LIBRARIES=OFF
  -DWARNINGS_AS_ERRORS=OFF
  -DBUILD_TESTING=OFF
  -DBUILD_CODE_COVERAGE=OFF
  -DBUILD_DOCUMENTATION=OFF
  -DRUN_LONG_UNIT_TESTS=OFF
  -DBUILD_PERFORMANCE_TESTS=OFF
  -DBUILD_STORAGE_SAMPLES=ON
  -DCPP_STANDARD=17
  -DCMAKE_BUILD_TYPE=Release)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)

# azure sdk
ExternalProject_Add(azure
  PREFIX azure
  GIT_REPOSITORY https://github.com/Azure/azure-sdk-for-cpp.git
  GIT_TAG azure-storage-files-datalake_12.2.0
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CMAKE_ARGS ${AZURE_CMAKE_BUILD_OPTIONS}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# set up properties for AZURE sdk library
ExternalProject_Get_Property(azure SOURCE_DIR)
ExternalProject_Get_Property(azure BINARY_DIR)

# assuming libxml2 is present
if(APPLE)
  set(LIBXML2_INCLUDE_DIR ${OPT_DIR}/libxml2/include)
  set(LIBXML2_LIBRARY_PATH ${OPT_DIR}/libxml2/lib/libxml2.dylib)
else()
  set(LIBXML2_INCLUDE_DIR /usr/include)
  set(LIBXML2_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libxml2.so)
endif()

set(LIBXML2_LIBRARY libxml2)
add_library(${LIBXML2_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${LIBXML2_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${LIBXML2_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${LIBXML2_INCLUDE_DIR}")

# alias of relative path under source or binary
set(CORE_PATH sdk/core/azure-core)
set(COMMON_PATH sdk/storage/azure-storage-common)
set(BLOBS_PATH sdk/storage/azure-storage-blobs)
set(DATALAKE_PATH sdk/storage/azure-storage-files-datalake)

# add azure core lib (include and lib)
set(AZURE_CORE_INCLUDE_DIR ${SOURCE_DIR}/${CORE_PATH}/inc)
file(MAKE_DIRECTORY ${AZURE_CORE_INCLUDE_DIR})
set(AZURE_CORE_LIBRARY_PATH ${BINARY_DIR}/${CORE_PATH}/libazure-core.a)
set(AZURE_CORE_LIBRARY azure_core)
add_library(${AZURE_CORE_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AZURE_CORE_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AZURE_CORE_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${AZURE_CORE_INCLUDE_DIR}")

# add azure storage common lib (include and lib)
set(AZURE_COMMON_INCLUDE_DIR ${SOURCE_DIR}/${COMMON_PATH}/inc)
file(MAKE_DIRECTORY ${AZURE_COMMON_INCLUDE_DIR})
set(AZURE_COMMON_LIBRARY_PATH ${BINARY_DIR}/${COMMON_PATH}/libazure-storage-common.a)
set(AZURE_COMMON_LIBRARY azure_common)
add_library(${AZURE_COMMON_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AZURE_COMMON_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AZURE_COMMON_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${AZURE_COMMON_INCLUDE_DIR}")

# add azure storage blobs lib (include and lib)
set(AZURE_BLOBS_INCLUDE_DIR ${SOURCE_DIR}/${BLOBS_PATH}/inc)
file(MAKE_DIRECTORY ${AZURE_BLOBS_INCLUDE_DIR})
set(AZURE_BLOBS_LIBRARY_PATH ${BINARY_DIR}/${BLOBS_PATH}/libazure-storage-blobs.a)
set(AZURE_BLOBS_LIBRARY azure_blobs)
add_library(${AZURE_BLOBS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AZURE_BLOBS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AZURE_BLOBS_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${AZURE_BLOBS_INCLUDE_DIR}")

# add azure data lake lib (include and lib)
set(AZURE_DATALAKE_INCLUDE_DIR ${SOURCE_DIR}/${DATALAKE_PATH}/inc)
file(MAKE_DIRECTORY ${AZURE_DATALAKE_INCLUDE_DIR})
set(AZURE_DATALAKE_LIBRARY_PATH ${BINARY_DIR}/${DATALAKE_PATH}/libazure-storage-files-datalake.a)
set(AZURE_DATALAKE_LIBRARY azure_datalake)
add_library(${AZURE_DATALAKE_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AZURE_DATALAKE_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AZURE_DATALAKE_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${AZURE_DATALAKE_INCLUDE_DIR}")
  
add_dependencies(${AZURE_DATALAKE_LIBRARY} azure)

# add a bundle
set(AZURE_LIBRARY azurelib)
add_library(${AZURE_LIBRARY} INTERFACE IMPORTED)
target_link_libraries(${AZURE_LIBRARY} 
  INTERFACE ${LIBXML2_LIBRARY}
  INTERFACE ${AZURE_CORE_LIBRARY}
  INTERFACE ${AZURE_COMMON_LIBRARY}
  INTERFACE ${AZURE_BLOBS_LIBRARY}
  INTERFACE ${AZURE_DATALAKE_LIBRARY})
