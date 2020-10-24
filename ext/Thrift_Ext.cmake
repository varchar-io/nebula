find_package(Threads REQUIRED)
include(ExternalProject)

# lib thrift name
set(THRIFT_LIBRARY libthrift)
add_custom_target(copyConfig)

if(APPLE)
  # MacOs has problem to build thrift with libcrypto
  # install thrift through brew
  # "brew install thrift"
  # thrift
  set(THRIFT_ROOT ${CELLAR_ROOT}/thrift/0.12.0/)
  set(THRIFT_INCLUDE_DIR ${THRIFT_ROOT}/include)
  set(THRIFT_LIBRARY_PATH ${THRIFT_ROOT}/lib/libthrift.a)
else()
  # build thrift
  SET(THRIFT_OPTS
    -DBUILD_TESTING=OFF
    -DBUILD_COMPILER=OFF
    -DBUILD_CPP=ON
    -DBUILD_TUTORIALS=OFF
    -DBUILD_AS3=OFF
    -DBUILD_C_GLIB=OFF
    -DBUILD_JAVA=OFF
    -DBUILD_PYTHON=OFF
    -DBUILD_HASKELL=OFF
    -DWITH_OPENSSL=ON
    -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
    -DBoost_INCLUDE_DIRS=${Boost_INCLUDE_DIRS}
    -DCMAKE_BUILD_TYPE=Release)

  ExternalProject_Add(thrift
    PREFIX thrift
    GIT_REPOSITORY https://github.com/apache/thrift.git
    GIT_TAG v0.12.0
    CMAKE_ARGS ${THRIFT_OPTS}
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
    DEPENDS ${OPENSSL_LIBRARY})

  # get source dir after download step
  ExternalProject_Get_Property(thrift SOURCE_DIR)
  ExternalProject_Get_Property(thrift BINARY_DIR)
  set(THRIFT_INCLUDE_DIR ${SOURCE_DIR}/lib/cpp/src)
  file(MAKE_DIRECTORY ${THRIFT_INCLUDE_DIR})
  set(THRIFT_LIBRARY_PATH ${BINARY_DIR}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}thrift.a)

  # during the build, it will generate one config.h in BINARY_DIR
  # we either can add that path to include path or copy this single file to SRC folder
  # configure_file is invoked in configuration step, we should use post build command
  # to copy the build output instead
  # configure_file(${THRIFT_INCLUDE_DIR}/thrift/ ${BINARY_DIR}/thrift/ COPYONLY)
  set(THRIFT_CONFIG ${BINARY_DIR}/thrift/config.h)
  add_custom_command(TARGET copyConfig POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${THRIFT_CONFIG} ${THRIFT_INCLUDE_DIR}/thrift/config.h)

endif()

add_library(${THRIFT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${THRIFT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${THRIFT_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${THRIFT_INCLUDE_DIR}")
add_dependencies(${THRIFT_LIBRARY} copyConfig)
