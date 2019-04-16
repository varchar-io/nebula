find_package(Threads REQUIRED)

# AWS SDK build options 
# https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/cmake-params.html
SET(AWS_CMAKE_BUILD_OPTIONS
  -DBUILD_SHARED_LIBS:BOOL=OFF
  -DBUILD_ONLY:STRING=s3)

# https://cmake.org/cmake/help/latest/module/ExternalProject.html
include(ExternalProject)
ExternalProject_Add(
  awsproj
  GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CMAKE_ARGS ${AWS_CMAKE_BUILD_OPTIONS}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# set up properties for AWS S3 module 
ExternalProject_Get_Property(awsproj SOURCE_DIR)
ExternalProject_Get_Property(awsproj BINARY_DIR)

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

add_dependencies(${AWS_CORE_LIBRARY} awsproj)

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
  
add_dependencies(${AWS_S3_LIBRARY} awsproj)