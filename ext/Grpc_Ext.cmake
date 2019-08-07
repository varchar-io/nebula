find_package(Threads REQUIRED)

include(ExternalProject)

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

# message("c-ares include: " ${CARES_INCLUDE_DIRS})
# message("c-ares lib: " ${CARES_LIBRARY_PATH})

# build protobuf
SET(PROTOBUF_OPTS
  -Dprotobuf_BUILD_TESTS:BOOL=OFF)

ExternalProject_Add(protobuf
  PREFIX protobuf
  GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
  SOURCE_SUBDIR cmake
  CMAKE_ARGS ${PROTOBUF_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

ExternalProject_Get_Property(protobuf SOURCE_DIR)
ExternalProject_Get_Property(protobuf BINARY_DIR)
set(PROTOBUF_INCLUDE_DIRS ${SOURCE_DIR}/src)
file(MAKE_DIRECTORY ${PROTOBUF_INCLUDE_DIRS})
set(PROTOBUF_LIBRARY_PATH ${BINARY_DIR}/libprotobuf.a)
set(PROTOBUF_LIBRARY libpb)
add_library(${PROTOBUF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${PROTOBUF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${PROTOBUF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${PROTOBUF_INCLUDE_DIRS}")
add_dependencies(${PROTOBUF_LIBRARY} protobuf)

# instlal protobuf, please go to the protobuf build folder
# and manually run the install there with sudo. 
# since it requires access, let's not turn it on in build script
# add_custom_target(install_protobuf
#       ALL COMMAND make install
#       WORKING_DIRECTORY ${BINARY_DIR}
#       DEPENDS ${PROTOBUF_LIBRARY})

# set PROTO compiler
SET(PROTO_COMPILER ${BINARY_DIR}/protoc)

# message("protobuf include: " ${PROTOBUF_INCLUDE_DIRS})
# message("protobuf lib: " ${PROTOBUF_LIBRARY_PATH})

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

# message("zlib include: " ${ZLIB_INCLUDE_DIRS})
# message("zlib lib: " ${ZLIB_LIBRARY_PATH})

# message("cmake current bin: " ${CMAKE_CURRENT_BINARY_DIR})


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
  ${_CMAKE_ARGS_OPENSSL_ROOT_DIR}
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
ExternalProject_Add(grpcweb
  PREFIX grpcweb
  GIT_REPOSITORY https://github.com/grpc/grpc-web.git
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

ExternalProject_Get_Property(grpcweb SOURCE_DIR)
ExternalProject_Get_Property(grpcweb BINARY_DIR)

# It's not cmake project, so use custom command to make the project
# build the proc-gen-grpc-web plugin so that we can use it to gen stubs later
SET(GRPC_WEB_PLUGIN "${SOURCE_DIR}/javascript/net/grpc/web/protoc-gen-grpc-web")
add_custom_target(build_grpcweb_plugin
      ALL COMMAND make plugin -I${PROTOBUF_INCLUDE_DIRS}
      WORKING_DIRECTORY ${SOURCE_DIR}
      DEPENDS ${PROTOBUF_LIBRARY} libgrpc grpcweb)
# target_compile_options(build_grpcweb_plugin PRIVATE -I${PROTOBUF_INCLUDE_DIRS})
