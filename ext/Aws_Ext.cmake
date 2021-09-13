find_package(Threads REQUIRED)

# AWS SDK build options 
# https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup.html
# https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/cmake-params.html
SET(AWS_CMAKE_BUILD_OPTIONS
  -DBUILD_SHARED_LIBS:BOOL=OFF
  -DENABLE_UNITY_BUILD=ON
  -DENABLE_TESTING=OFF
  -DENABLE_FUNCTIONAL_TESTING=OFF
  -DBUILD_DEPS:BOOL=ON
  -DBUILD_ONLY:STRING=s3
  -DCPP_STANDARD=17
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_CXX_FLAGS=-Wno-error=deprecated-declarations
  -DCMAKE_CXX_FLAGS=-Wno-error=uninitialized
  -DCUSTOM_MEMORY_MANAGEMENT=0)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
# aws sdk
ExternalProject_Add(aws
  PREFIX aws
  GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG 1.9.30
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CMAKE_ARGS ${AWS_CMAKE_BUILD_OPTIONS}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# set up properties for AWS S3 module
ExternalProject_Get_Property(aws SOURCE_DIR)
ExternalProject_Get_Property(aws BINARY_DIR)

# CRT ROOT
set(CRT_SRC_ROOT ${SOURCE_DIR}/crt/aws-crt-cpp/crt)
set(CRT_LIB_ROOT ${BINARY_DIR}/crt/aws-crt-cpp/crt)

# set up dependencies - io
set(AWS_IO_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-io/include)
file(MAKE_DIRECTORY ${AWS_IO_INCLUDE_DIR})
set(AWS_IO_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-io/libaws-c-io.a)
set(AWS_IO_LIBRARY awsio)
add_library(${AWS_IO_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_IO_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_IO_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_IO_INCLUDE_DIR}")

# set up dependencies - mqtt
set(AWS_MQTT_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-mqtt/include)
file(MAKE_DIRECTORY ${AWS_MQTT_INCLUDE_DIR})
set(AWS_MQTT_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-mqtt/libaws-c-mqtt.a)
set(AWS_MQTT_LIBRARY awsmqtt)
add_library(${AWS_MQTT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_MQTT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_MQTT_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_MQTT_INCLUDE_DIR}")

# set up dependencies - CS3
set(AWS_CS3_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-s3/include)
file(MAKE_DIRECTORY ${AWS_CS3_INCLUDE_DIR})
set(AWS_CS3_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-s3/libaws-c-s3.a)
set(AWS_CS3_LIBRARY awscs3)
add_library(${AWS_CS3_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_CS3_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_CS3_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_CS3_INCLUDE_DIR}")

# set up dependencies - http
set(AWS_HTTP_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-http/include)
file(MAKE_DIRECTORY ${AWS_HTTP_INCLUDE_DIR})
set(AWS_HTTP_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-http/libaws-c-http.a)
set(AWS_HTTP_LIBRARY awshttp)
add_library(${AWS_HTTP_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_HTTP_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_HTTP_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_HTTP_INCLUDE_DIR}")

# set up dependencies - cal
set(AWS_CAL_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-cal/include)
file(MAKE_DIRECTORY ${AWS_CAL_INCLUDE_DIR})
set(AWS_CAL_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-cal/libaws-c-cal.a)
set(AWS_CAL_LIBRARY awscal)
add_library(${AWS_CAL_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_CAL_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_CAL_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_CAL_INCLUDE_DIR}")

# set up dependencies - auth
set(AWS_AUTH_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-auth/include)
file(MAKE_DIRECTORY ${AWS_AUTH_INCLUDE_DIR})
set(AWS_AUTH_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-auth/libaws-c-auth.a)
set(AWS_AUTH_LIBRARY awsauth)
add_library(${AWS_AUTH_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_AUTH_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_AUTH_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_AUTH_INCLUDE_DIR}")

# set up dependencies - common
set(AWS_COMMON_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-common/include ${CRT_LIB_ROOT}/aws-c-common/generated/include)
file(MAKE_DIRECTORY ${AWS_COMMON_INCLUDE_DIR})
set(AWS_COMMON_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-common/libaws-c-common.a)
set(AWS_COMMON_LIBRARY awscommon)
add_library(${AWS_COMMON_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_COMMON_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_COMMON_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_COMMON_INCLUDE_DIR}")

# set up dependencies - compression
set(AWS_COMPRESSION_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-compression/include)
file(MAKE_DIRECTORY ${AWS_COMPRESSION_INCLUDE_DIR})
set(AWS_COMPRESSION_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-c-compression/libaws-c-compression.a)
set(AWS_COMPRESSION_LIBRARY awscompression)
add_library(${AWS_COMPRESSION_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_COMPRESSION_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_COMPRESSION_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_COMPRESSION_INCLUDE_DIR}")

# set up dependencies - checksums
set(AWS_CHECKSUMS_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-checksums/include)
file(MAKE_DIRECTORY ${AWS_CHECKSUMS_INCLUDE_DIR})
set(AWS_CHECKSUMS_LIBRARY_PATH ${CRT_LIB_ROOT}/aws-checksums/libaws-checksums.a)
set(AWS_CHECKSUMS_LIBRARY awschecksums)
add_library(${AWS_CHECKSUMS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_CHECKSUMS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_CHECKSUMS_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_CHECKSUMS_INCLUDE_DIR}")

# set up dependencies - eventstream
set(AWS_EVENTSTREAM_INCLUDE_DIR ${CRT_SRC_ROOT}/aws-c-event-stream/include)
file(MAKE_DIRECTORY ${AWS_EVENTSTREAM_INCLUDE_DIR})
set(AWS_EVENTSTREAM_LIBRARY_PATH ${BINARY_DIR}/lib/libaws-c-event-stream.a)
set(AWS_EVENTSTREAM_LIBRARY awseventstream)
add_library(${AWS_EVENTSTREAM_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_EVENTSTREAM_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_EVENTSTREAM_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_EVENTSTREAM_INCLUDE_DIR}")

# set up dependencies - s2n
set(AWS_S2N_LIBRARY_PATH ${BINARY_DIR}/lib/libs2n.a)
set(AWS_S2N_LIBRARY awss2n)
add_library(${AWS_S2N_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_S2N_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_S2N_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}")

# set up dependencies - crt
set(AWS_CRT_INCLUDE_DIR ${SOURCE_DIR}/crt/aws-crt-cpp/include)
file(MAKE_DIRECTORY ${AWS_CRT_INCLUDE_DIR})
set(AWS_CRT_LIBRARY_PATH ${BINARY_DIR}/crt/aws-crt-cpp/libaws-crt-cpp.a)
set(AWS_CRT_LIBRARY awscrt)
add_library(${AWS_CRT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${AWS_CRT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${AWS_CRT_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${AWS_CRT_INCLUDE_DIR}")

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
  set(CURL_INCLUDE_DIRS ${OPT_DIR}/curl/include)
  set(CURL_LIBRARY_PATH ${OPT_DIR}/curl/lib/libcurl.dylib)
else()
  set(CURL_INCLUDE_DIRS /usr/include)
  # to link curl lib statically, use curl-config to see what is needed
  # /usr/bin/curl-config --cflags --static-libs
  set(CURL_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libcurl.so)
endif()

set(CURL_LIBRARY curl)
add_library(${CURL_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${CURL_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${CURL_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES"  "${CURL_INCLUDE_DIRS}")

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
  set(AWS_FRAMEWORK "-framework corefoundation -lresolv -L${OPT_DIR}/icu4c/lib")
endif()

# add a bundle
set(AWS_LIBRARY awslib)
add_library(${AWS_LIBRARY} INTERFACE IMPORTED)
target_link_libraries(${AWS_LIBRARY} 
  INTERFACE ${AWS_S3_LIBRARY}
  INTERFACE ${AWS_CORE_LIBRARY}
  INTERFACE ${AWS_CRT_LIBRARY}
  INTERFACE ${AWS_EVENTSTREAM_LIBRARY}
  INTERFACE ${AWS_CS3_LIBRARY}
  INTERFACE ${AWS_AUTH_LIBRARY}
  INTERFACE ${AWS_CAL_LIBRARY}
  INTERFACE ${AWS_MQTT_LIBRARY}
  INTERFACE ${AWS_HTTP_LIBRARY}
  INTERFACE ${AWS_COMPRESSION_LIBRARY}
  INTERFACE ${AWS_CHECKSUMS_LIBRARY}
  INTERFACE ${AWS_IO_LIBRARY}
  INTERFACE ${AWS_COMMON_LIBRARY}
  INTERFACE ${AWS_S2N_LIBRARY}
  INTERFACE ${CURL_LIBRARY}
  INTERFACE ${OPENSSL_LIBRARY}
  INTERFACE ${CRYPTO_LIBRARY}
  ${AWS_FRAMEWORK})
