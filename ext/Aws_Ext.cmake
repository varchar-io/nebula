find_package(Threads REQUIRED)

# this project may require Curl, install curl library using this command works
# "sudo apt-get install libcurl4-openssl-dev"

# AWS SDK build options 
# https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup.html
# https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/cmake-params.html
SET(AWS_CMAKE_BUILD_OPTIONS
  -DBUILD_SHARED_LIBS:BOOL=OFF
  -DENABLE_UNITY_BUILD=ON
  -DBUILD_DEPS:BOOL=ON
  -DBUILD_ONLY:STRING=s3
  -DCPP_STANDARD=17
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_CXX_FLAGS=-Wno-error=deprecated-declarations
  -DCUSTOM_MEMORY_MANAGEMENT=0)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
# aws sdk
ExternalProject_Add(aws
  PREFIX aws
  GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG 1.7.344
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CMAKE_ARGS ${AWS_CMAKE_BUILD_OPTIONS}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# set up properties for AWS S3 module
ExternalProject_Get_Property(aws SOURCE_DIR)
ExternalProject_Get_Property(aws BINARY_DIR)

# set up dependencies - common
set(DEP_ROOT ${BINARY_DIR}/.deps/install)
set(AWS_COMMON_INCLUDE_DIR ${DEP_ROOT}/include)
file(MAKE_DIRECTORY ${AWS_COMMON_INCLUDE_DIR})
set(AWS_COMMON_LIBRARY_PATH ${DEP_ROOT}/lib/libaws-c-common.a)
set(AWS_COMMON_LIBRARY awscommon)
add_library(${AWS_COMMON_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_COMMON_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_COMMON_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_COMMON_INCLUDE_DIR}")

# set up dependencies - checksums
set(AWS_CHECKSUMS_INCLUDE_DIR ${DEP_ROOT}/include)
set(AWS_CHECKSUMS_LIBRARY_PATH ${DEP_ROOT}/lib/libaws-checksums.a)
set(AWS_CHECKSUMS_LIBRARY awschecksums)
add_library(${AWS_CHECKSUMS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_CHECKSUMS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_CHECKSUMS_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_CHECKSUMS_INCLUDE_DIR}")

# set up dependencies - eventstream
set(AWS_EVENTSTREAM_INCLUDE_DIR ${DEP_ROOT}/include)
set(AWS_EVENTSTREAM_LIBRARY_PATH ${DEP_ROOT}/lib/libaws-c-event-stream.a)
set(AWS_EVENTSTREAM_LIBRARY awseventstream)
add_library(${AWS_EVENTSTREAM_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_EVENTSTREAM_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_EVENTSTREAM_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_EVENTSTREAM_INCLUDE_DIR}")

# add AWS core
set(AWS_CORE_INCLUDE_DIRS ${SOURCE_DIR}/aws-cpp-sdk-core/include)
file(MAKE_DIRECTORY ${AWS_CORE_INCLUDE_DIRS})
set(AWS_CORE_LIBRARY_PATH ${BINARY_DIR}/aws-cpp-sdk-core/libaws-cpp-sdk-core.a)

set(AWS_CORE_LIBRARY awscore)
add_library(${AWS_CORE_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_CORE_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_CORE_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_CORE_INCLUDE_DIRS}")

add_dependencies(${AWS_CORE_LIBRARY} aws)

# add AWS S3
set(AWS_S3_INCLUDE_DIRS ${SOURCE_DIR}/aws-cpp-sdk-s3/include)
file(MAKE_DIRECTORY ${AWS_S3_INCLUDE_DIRS})
set(AWS_S3_LIBRARY_PATH ${BINARY_DIR}/aws-cpp-sdk-s3/libaws-cpp-sdk-s3.a)
set(AWS_S3_LIBRARY awss3)
add_library(${AWS_S3_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_S3_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_S3_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${AWS_S3_INCLUDE_DIRS}")
  
add_dependencies(${AWS_S3_LIBRARY} aws)

# add curl lib
# this command return all supported protocols
# `/usr/local/curl/bin/curl-config --protocols`
# or `/opt/local/bin/curl-config --protocols`
if(APPLE)
  set(CURL_INCLUDE_DIRS /opt/local/include)
  set(CURL_LIBRARY_PATH /opt/local/lib/libcurl.dylib)
else()
  set(CURL_INCLUDE_DIRS /usr/include)
  # to link curl lib statically, use curl-config to see what is needed
  # /usr/bin/curl-config --cflags --static-libs
  set(CURL_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libcurl.a)
endif()

set(CURL_LIBRARY curl)
add_library(${CURL_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${CURL_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${CURL_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${CURL_INCLUDE_DIRS}")

# NOTE: libcurl4-openssl version has problem to build on my dev server
# I end up using "sudo apt-get install libcurl4-gnutls-dev" instead. 
# Also removed openssl version by "sudo apt-get remove libcurl4-openssl-dev"
# libcurl depends on lots of other libraries, here is link flags requires
# -lgnutls -lgcrypt -lz -lidn -lgssapi_krb5 -lldap -llber -lcom_err -lrtmp
if(NOT APPLE)
  target_link_libraries(${CURL_LIBRARY} 
    INTERFACE gnutls 
    INTERFACE gcrypt 
    INTERFACE z 
    INTERFACE idn2 
    INTERFACE gssapi_krb5 
    INTERFACE ldap 
    INTERFACE lber 
    INTERFACE com_err 
    INTERFACE rtmp
    INTERFACE nettle
    INTERFACE nghttp2 
    INTERFACE psl)
endif()

if(APPLE)
  # libresolve is needed by libcares.a - why put it here?
  # because we want 2 special APPLE libraries sounding hacky.
  set(AWS_FRAMEWORK "-framework corefoundation /usr/lib/libresolv.dylib")
endif()

# add a bundle
set(AWS_LIBRARY awslib)
add_library(${AWS_LIBRARY} INTERFACE IMPORTED)
target_link_libraries(${AWS_LIBRARY} 
  INTERFACE ${AWS_S3_LIBRARY}
  INTERFACE ${AWS_CORE_LIBRARY}
  INTERFACE ${AWS_EVENTSTREAM_LIBRARY}
  INTERFACE ${AWS_COMMON_LIBRARY}
  INTERFACE ${AWS_CHECKSUMS_LIBRARY}
  INTERFACE ${CURL_LIBRARY}
  INTERFACE ${OPENSSL_LIBRARY}
  INTERFACE ${CRYPTO_LIBRARY}
  ${AWS_FRAMEWORK})
