find_package(Threads REQUIRED)

include(ExternalProject)

# grpc doesn't include stub generator for java, we get it from grpc-java repo
# use 1.25.0 for now to fit some consumer - we can upgrade it to newer version later
ExternalProject_Add(grpc-java
  PREFIX grpc-java
  GIT_REPOSITORY https://github.com/grpc/grpc-java.git
  GIT_TAG v1.25.0
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

  ExternalProject_Get_Property(grpc-java SOURCE_DIR)
  add_custom_target(build-grpc-java-compiler ALL
        COMMAND ../gradlew java_pluginExecutable -PskipAndroid=true
        WORKING_DIRECTORY ${SOURCE_DIR}/compiler
        DEPENDS grpc-java)

  SET(GRPC_JAVA_PLUGIN ${SOURCE_DIR}/compiler/build/exe/java_plugin/protoc-gen-grpc-java)

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
# NOTE - latest flatbuffers is not in sync with latest grpc.
# I made a local change to solve the the build break:
# 1. ~/grpc/src/grpc/include/grpcpp/impl/codegen/byte_buffer.h
#   move method to public grpc_byte_buffer* c_buffer() { return buffer_; }
# 2. ~/flatbuffers/src/flatbuffers/include/flatbuffers/grpc.h
#   use ByteBuffer as parameter type of Deserialize
#     + static grpc::Status Deserialize(ByteBuffer *bb, flatbuffers::grpc::Message<T> *msg) {
#     + grpc_byte_buffer* buffer = nullptr;
#     + if (!bb || !(buffer = bb->c_buffer())) {
#         return ::grpc::Status(::grpc::StatusCode::INTERNAL, "No payload");
#       }
ExternalProject_Add(flatbuffers
  PREFIX flatbuffers
  GIT_REPOSITORY https://github.com/google/flatbuffers.git
  GIT_TAG v1.12.0
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

# -DgRPC_PROTOBUF_PROVIDER:STRING=package
# -DgRPC_PROTOBUF_PACKAGE_TYPE:STRING=CONFIG
# -DProtobuf_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/protobuf
# -DgRPC_ZLIB_PROVIDER:STRING=package
# -DZLIB_ROOT:STRING=${CMAKE_CURRENT_BINARY_DIR}/zlib
# -DgRPC_CARES_PROVIDER:STRING=package
# -Dc-ares_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/c-ares/src/c-ares-build/
# -DgRPC_SSL_PROVIDER:STRING=package

# build grpc
# torelate the build of grpc which uses boringssl that uses golang to execute tests
# unless we add boringssl build here as external dependency as well
# Install golang here https://golang.org/doc/install?download=go1.12.4.darwin-amd64.pkg for mac

SET(GRPC_CMAKE_ARGS
  -DgRPC_BUILD_CSHARP_EXT:BOOL=OFF
  -DgRPC_INSTALL:BOOL=OFF
  -DgRPC_BUILD_TESTS:BOOL=OFF
  -DgRPC_SSL_PROVIDER:STRING=package
  -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/grpc)

# use specific version since flatbuffers not catching up
# or submit PR to fix ByteBuffer used in Deserialize method
ExternalProject_Add(grpc
  PREFIX grpc
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  GIT_TAG v1.20.0
  CMAKE_CACHE_ARGS ${GRPC_CMAKE_ARGS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

ExternalProject_Get_Property(grpc SOURCE_DIR)
ExternalProject_Get_Property(grpc BINARY_DIR)
set(GRPC_INCLUDE_DIRS ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${GRPC_INCLUDE_DIRS})

# list all libries here through this shell command
# ./build> ll grpc/src/grpc-build/*.a | cut -d'/' -f4 | sed -e 's/^/\$\{BINARY_DIR\}\//'
# use their file name as the LIB target name
foreach(_lib
  libaddress_sorting libgpr libgrpc libgrpc_cronet libgrpc_plugin_support libgrpc_unsecure libgrpcpp_channelz
  libgrpc++ libgrpc++_cronet libgrpc++_reflection libgrpc++_unsecure libgrpc++_error_details)
  add_library(${_lib} UNKNOWN IMPORTED)
  set_target_properties(${_lib} PROPERTIES
      "IMPORTED_LOCATION" "${BINARY_DIR}/${_lib}.a"
      "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
      "INTERFACE_INCLUDE_DIRECTORIES" "${GRPC_INCLUDE_DIRS}")
  add_dependencies(${_lib} 
      ${CARES_LIBRARY}
      ${ZLIB_LIBRARY}
      ${PROTOBUF_LIBRARY}
      grpc)
endforeach()

# also add all useful executable out of the build
# used in protobuf build
SET(GRPC_CPP_PLUGIN ${BINARY_DIR}/grpc_cpp_plugin)
SET(GRPC_NODE_PLUGIN ${BINARY_DIR}/grpc_node_plugin)


# a few more installation steps to ensure tools works before try out examples
# Start https://grpc.io/docs/quickstart/cpp.html
# 0. install pkg-config
#    mac version: brew install pkg-config
# 1. install protobuf to local machine
#    go to ./build/protobuf/src/protobuf-build and run "sudo make install"
# 2. install grpc to local machine
#    go to ./build/grpc/src/grpc-build and run "sudo make install"
# 3. try out examples
#    go to folder ./build/grpc/src/grpc/examples/cpp/helloword and "make"

# Below section is used to introduce GRPC_WEB to enable web endpoint

# download the project only
# ExternalProject_Add(grpcweb
#   PREFIX grpcweb
#   GIT_REPOSITORY https://github.com/grpc/grpc-web.git
#   CONFIGURE_COMMAND ""
#   BUILD_COMMAND ""
#   UPDATE_COMMAND ""
#   INSTALL_COMMAND ""
#   LOG_DOWNLOAD ON
#   LOG_CONFIGURE ON
#   LOG_BUILD ON)

# ExternalProject_Get_Property(grpcweb SOURCE_DIR)
# ExternalProject_Get_Property(grpcweb BINARY_DIR)

# It's not cmake project, so use custom command to make the project
# build the proc-gen-grpc-web plugin so that we can use it to gen stubs later
# SET(GRPC_WEB_PLUGIN "${SOURCE_DIR}/javascript/net/grpc/web/protoc-gen-grpc-web")
# add_custom_target(build_grpcweb_plugin
#       ALL COMMAND make plugin -I${PROTOBUF_INCLUDE_DIRS}
#       WORKING_DIRECTORY ${SOURCE_DIR}
#       DEPENDS ${PROTOBUF_LIBRARY} libgrpc grpcweb)
# target_compile_options(build_grpcweb_plugin PRIVATE -I${PROTOBUF_INCLUDE_DIRS})
