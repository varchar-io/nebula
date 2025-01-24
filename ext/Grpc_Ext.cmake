find_package(Threads REQUIRED)

include(ExternalProject)

# grpc doesn't include stub generator for java, we get it from grpc-java repo
# use 1.25.0 for now to fit some consumer - we can upgrade it to newer version later
ExternalProject_Add(grpc-java
  PREFIX grpc-java
  GIT_REPOSITORY https://github.com/grpc/grpc-java.git
  GIT_TAG v1.40.1
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# seems GRPC-java not working on ARM yet
if(ARM STREQUAL "0")
  ExternalProject_Get_Property(grpc-java SOURCE_DIR)
  add_custom_target(build-grpc-java-compiler ALL
        COMMAND ../gradlew java_pluginExecutable -PskipAndroid=true
        WORKING_DIRECTORY ${SOURCE_DIR}/compiler
        DEPENDS grpc-java ${PROTOBUF_LIBRARY})

  SET(GRPC_JAVA_PLUGIN ${SOURCE_DIR}/compiler/build/exe/java_plugin/protoc-gen-grpc-java)
endif()

# build cares
SET(CARES_OPTS
  -DCARES_STATIC:BOOL=ON
  -DCARES_SHARED:BOOL=OFF
  -DCARES_STATIC_PIC:BOOL=OFF
  -DCARES_INSTALL:BOOL=OFF
  -DCARES_BUILD_TOOLS:BOOL=OFF)

ExternalProject_Add(c-ares
  PREFIX c-ares
  GIT_REPOSITORY https://github.com/c-ares/c-ares.git
  GIT_TAG cares-1_17_2
  CMAKE_ARGS ${CARES_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

ExternalProject_Get_Property(c-ares SOURCE_DIR)
ExternalProject_Get_Property(c-ares BINARY_DIR)
set(CARES_INCLUDE_DIRS ${SOURCE_DIR})
file(MAKE_DIRECTORY ${CARES_INCLUDE_DIRS})
set(CARES_LIBRARY_PATH ${BINARY_DIR}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}cares.a)
set(CARES_LIBRARY cares)
add_library(${CARES_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${CARES_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${CARES_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${CARES_INCLUDE_DIRS}")
add_dependencies(${CARES_LIBRARY} c-ares)

# build flatbuffers which is used for internal communications between nodes
# to enable flatc available on the machine, just do "make install" in the build folder
SET(FB_OPTS
  -DFLATBUFFERS_BUILD_TESTS:BOOL=OFF
  -DCMAKE_CXX_FLAGS=-Wno-error=unused-but-set-variable)
ExternalProject_Add(flatbuffers
  PREFIX flatbuffers
  GIT_REPOSITORY https://github.com/google/flatbuffers.git
  GIT_TAG v2.0.0
  CMAKE_ARGS ${FB_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# use FB as acron for flatbuffers
ExternalProject_Get_Property(flatbuffers SOURCE_DIR)
ExternalProject_Get_Property(flatbuffers BINARY_DIR)
set(FB_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${FB_INCLUDE_DIRS})
set(FB_LIBRARY_PATH ${BINARY_DIR}/libflatbuffers.a)
set(FLATBUFFERS_LIBRARY libflatbuffers)
add_library(${FLATBUFFERS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${FLATBUFFERS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${FB_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${FB_INCLUDE_DIRS}")
add_dependencies(${FLATBUFFERS_LIBRARY} flatbuffers)

# set flatbuffers compiler
SET(FLATBUFFERS_COMPILER ${BINARY_DIR}/flatc)

# build grpc
# torelate the build of grpc which uses boringssl that uses golang to execute tests
# unless we add boringssl build here as external dependency as well
# Install golang here https://golang.org/doc/install?download=go1.12.4.darwin-amd64.pkg for mac

SET(GRPC_CMAKE_ARGS
  -DCMAKE_CXX_STANDARD:STRING=17
  -DgRPC_BUILD_CSHARP_EXT:BOOL=OFF
  -DgRPC_BUILD_TESTS:BOOL=OFF
  -DgRPC_INSTALL:BOOL=OFF
  -DgRPC_ABSL_PROVIDER:STRING=package
  -DgRPC_PROTOBUF_PROVIDER:STRING=package
  -DgRPC_SSL_PROVIDER:STRING=package
  -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/grpc)

# unfortunately ssllib in latest macos doesn't allow application to use
# they are internal used only - pointing to the version installed by brew
if(APPLE)
  SET(GRPC_CMAKE_ARGS ${GRPC_CMAKE_ARGS} "-DOPENSSL_ROOT_DIR:STRING=${OPT_DIR}/openssl")
else()
  SET(GRPC_CMAKE_ARGS ${GRPC_CMAKE_ARGS} "-DOPENSSL_ROOT_DIR:STRING=/usr/local/openssl")
endif()

# use specific version since flatbuffers not catching up
# or submit PR to fix ByteBuffer used in Deserialize method
ExternalProject_Add(grpc
  PREFIX grpc
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  GIT_TAG v1.70.0
  CMAKE_CACHE_ARGS ${GRPC_CMAKE_ARGS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

add_dependencies(grpc libpb)

ExternalProject_Get_Property(grpc SOURCE_DIR)
ExternalProject_Get_Property(grpc BINARY_DIR)
set(GRPC_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${GRPC_INCLUDE_DIRS})

# list all libries here through this shell command
# ./build> ll grpc/src/grpc-build/*.a | cut -d'/' -f4 | sed -e 's/^/\$\{BINARY_DIR\}\//'
# use their file name as the LIB target name
foreach(_lib
  libaddress_sorting
  libgpr
  libgrpc++
  libgrpc++_alts
  libgrpc++_error_details
  libgrpc++_reflection
  libgrpc++_unsecure
  libgrpc
  libgrpc_authorization_provider
  libgrpc_plugin_support
  libgrpc_unsecure
  libgrpcpp_channelz
  libupb_base_lib
  libupb_json_lib
  libupb_mem_lib
  libupb_message_lib
  libupb_mini_descriptor_lib
  libupb_textformat_lib
  libupb_wire_lib)
  add_library(${_lib} UNKNOWN IMPORTED)
  set_target_properties(${_lib} PROPERTIES
      "IMPORTED_LOCATION" "${BINARY_DIR}/${_lib}.a"
      "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
      "INTERFACE_INCLUDE_DIRECTORIES" "${GRPC_INCLUDE_DIRS}")
  add_dependencies(${_lib}
      ${CARES_LIBRARY}
      ${ZLIB_LIBRARY}
      ${PROTOBUF_LIBRARY}
      ${RE2_LIBRARY}
      grpc)
endforeach()

# also add all useful executable out of the build
# used in protobuf build
set(GRPC_CPP_PLUGIN ${BINARY_DIR}/grpc_cpp_plugin)
set(GRPC_NODE_PLUGIN ${BINARY_DIR}/grpc_node_plugin)
