set(NEBULA_SERVICE NService)

# ask for gflags
include_directories(include ${GFLAGS_INCLUDE_DIRS})

# ask for glog
include_directories(include ${GLOG_INCLUDE_DIRS})

# it depends on fmt
include_directories(include ${FMT_INCLUDE_DIRS})

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})

# build hello world examples
# locate helloworld service proto file
SET(SERVICE_DIR "${NEBULA_SRC}/service")

# A folder holding temporary generated files during build
# add this entry to .gitignore in root to exclude these files to be commited and checked in
SET(GEN_DIR "${SERVICE_DIR}/gen/nebula")
file(MAKE_DIRECTORY ${GEN_DIR})

# make a placeholder file to be built
SET(NSERVER "${CMAKE_CURRENT_BINARY_DIR}/NebulaServer")
set(NNSERVER "${CMAKE_CURRENT_BINARY_DIR}/NodeServer")
message("Nebula Server: ${NSERVER}")
file(TOUCH ${NSERVER})
file(TOUCH ${NNSERVER})

get_filename_component(nproto "${SERVICE_DIR}/protos/nebula.proto" ABSOLUTE)
get_filename_component(nproto_path "${nproto}" PATH)

# Generated sources from proto file
set(nproto_srcs "${GEN_DIR}/nebula.pb.cc")
set(nproto_hdrs "${GEN_DIR}/nebula.pb.h")
set(ngrpc_srcs "${GEN_DIR}/nebula.grpc.pb.cc")
set(ngrpc_hdrs "${GEN_DIR}/neubla.grpc.pb.h")
add_custom_command(
      OUTPUT "${nproto_srcs}" "${nproto_hdrs}" "${ngrpc_srcs}" "${ngrpc_hdrs}"
      COMMAND ${PROTO_COMPILER}
      ARGS --grpc_out "${GEN_DIR}"
        --cpp_out "${GEN_DIR}"
        -I "${nproto_path}"
        --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
        "${nproto}"
      DEPENDS ${nproto})

# Include generated *.pb.h files
include_directories("${GEN_DIR}")

# generate a node client for this service
SET(NODE_GEN "${GEN_DIR}/nodejs")
file(MAKE_DIRECTORY ${NODE_GEN})
add_custom_target(nebula_node_client ALL 
  COMMAND ${PROTO_COMPILER}
      --grpc_out="${NODE_GEN}"
      --js_out=import_style=commonjs,binary:${NODE_GEN}
      -I "${nproto_path}"
      --plugin=protoc-gen-grpc="${GRPC_NODE_PLUGIN}"
      "${nproto}"
DEPENDS ${nproto})

# build flatbuffers schema
get_filename_component(nfbs "${SERVICE_DIR}/fbs/node.fbs" ABSOLUTE)
get_filename_component(nfbs_path "${nfbs}" PATH)

# Generated sources from proto file
set(NODE_GEN_DIR "${GEN_DIR}/node")
file(MAKE_DIRECTORY ${NODE_GEN_DIR})
add_custom_target(compile_fbs ALL 
  COMMAND ${FLATBUFFERS_COMPILER} -b -o "${NODE_GEN_DIR}" --cpp --grpc "${nfbs}"
DEPENDS ${nfbs})

# Include generated *.pb.h files
set(nodegrpc_srcs "${NODE_GEN_DIR}/node.grpc.fb.cc")
file(TOUCH ${nodegrpc_srcs})

# build everything else as library except executable of NebulaServer and NebulaClient
add_library(${NEBULA_SERVICE} STATIC 
    ${NEBULA_SRC}/service/node/ConnectionPool.cpp
    ${NEBULA_SRC}/service/node/NodeClient.cpp
    ${NEBULA_SRC}/service/nebula/QueryHandler.cpp
    ${NEBULA_SRC}/service/nebula/NebulaService.cpp
    ${nproto_srcs}
    ${ngrpc_srcs}
    ${nodegrpc_srcs})
target_link_libraries(${NEBULA_SERVICE}
    PRIVATE ${NEBULA_API}
    PRIVATE libgrpc++
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${XXH_LIBRARY}
    PRIVATE ${JSON_LIBRARY}
    PRIVATE ${PROTOBUF_LIBRARY}
    PRIVATE ${FLATBUFFERS_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})
target_compile_options(${NEBULA_SERVICE} PRIVATE -Wno-error=unused-parameter)
if(APPLE)
    target_compile_options(${NEBULA_SERVICE} 
      PRIVATE -Wno-error=unknown-warning-option
      PRIVATE -Wno-unused-parameter)
endif()

# Targets: 
#   nebula/NebulaClient
#   nebula/NebulaServer
#   node/NodeServer
list(APPEND dirs "nebula")
list(APPEND dirs "nebula")
list(APPEND dirs "node")
list(APPEND targets "NebulaClient")
list(APPEND targets "NebulaServer")
list(APPEND targets "NodeServer")
list(LENGTH dirs count)
math(EXPR list_max_index ${count}-1)
foreach(i RANGE ${list_max_index})
  list(GET dirs ${i} dir)
  list(GET targets ${i} target)

  # add the executable target
  add_executable(${target} "${SERVICE_DIR}/${dir}/${target}.cpp")

  # NOTE that - on linux, GCC behaves wired, the order of the dependencies matter
  # which means, libgrpc++ depends on libgrpc, and likewise, libgrpc depends on libgpr and address_sorting
  # if we messed up the order, the link will report huge amount of errors like undefined referneces.
  target_link_libraries(${target}
    PRIVATE ${NEBULA_SERVICE}
    PRIVATE libgrpc++
    PRIVATE libgrpc
    PRIVATE libgpr
    PRIVATE libaddress_sorting
    PRIVATE ${FOLLY_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${CARES_LIBRARY}
    PRIVATE ${ZLIB_LIBRARY}
    PRIVATE ${XXH_LIBRARY}
    PRIVATE ${JSON_LIBRARY}
    PRIVATE ${OMM_LIBRARY}
    PRIVATE ${PROTOBUF_LIBRARY}
    PRIVATE ${FLATBUFFERS_LIBRARY}
    PRIVATE ${OPENSSL_LIBRARY}
    PRIVATE ${CRYPTO_LIBRARY})
    
    # disalbe warning into errors for due to these generated files
    target_compile_options(${target} PRIVATE -Wno-error=unused-parameter)

    # add post-build event to copy binary into gen folder
    add_custom_command(TARGET ${target} POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_BINARY_DIR}/${target}"
        "${GEN_DIR}/${target}")
endforeach()

# configure_file is good as it is senstive on the input, when input changes, output will be updated
configure_file(${NSERVER} ${GEN_DIR}/NebulaServer COPYONLY)
configure_file(${NNSERVER} ${GEN_DIR}/NodeServer COPYONLY)

# build web client library for nebula
add_custom_target(nebula_web_client ALL 
  COMMAND ${PROTO_COMPILER} 
        --js_out=import_style=commonjs:"${GEN_DIR}"
        -I "${nproto_path}"
        --grpc-web_out=import_style=commonjs,mode=grpcwebtext:"${GEN_DIR}"
        --plugin=protoc-gen-grpc-web=${GRPC_WEB_PLUGIN}
        "${nproto}"
      WORKING_DIRECTORY ${SERVICE_DIR}
      DEPENDS ${nproto})

# To run the service together with a http proxy to serve web requests
# we use grpc-web implementations and follow their instructions to set this up
# ON mac (assuming linux has easier setup or already there)
# https://stackoverflow.com/questions/44084846/cannot-connect-to-the-docker-daemon-on-macos
# 1. Install docker app "brew cask install docker"
# 2. Launch the docker app. (allow the privileage it asks for) - a whale icon shall show on top bar
# 3. run docker command now (rather than "brew install docker")
# run the proxy in this way
# $ docker build -t nebula/envoy -f http/envoy.Dockerfile .
# $ docker run -d -p 8080:8080 nebula/envoy
# get_filename_component(docker_envoy "${SERVICE_DIR}/http/envoy.Dockerfile" ABSOLUTE)
# add_custom_target(envoydocker
#       ALL COMMAND docker build -t nebula/envoy -f ${docker_envoy} .
#       WORKING_DIRECTORY ${SERVICE_DIR}/http
#       DEPENDS ${docker_envoy})

# use docker-compose to build up the service and run them together
# add_custom_target(docker-compose
#       ALL COMMAND docker-compose build -t nebula/envoy -f ${docker_envoy} .
#       WORKING_DIRECTORY ${SERVICE_DIR}
#       DEPENDS ${docker_envoy})

# we will also run a simple web server to serve the page which hosts our client.js logic
# inside the client.js logic, it will call into 8080 port for data request through envoy proxy
# envoy proxy will contact 9190 port where is the real server for response.
# to run the web server, first we pack all js file into a single one
# here we are using webpack:
# $ npm install (based on package.json - can be reused for all service)
# $ npx webpack client.js

# build test binary
add_executable(ServiceTests 
  ${NEBULA_SRC}/service/test/TestQueryHandler.cpp
  ${NEBULA_SRC}/service/test/TestQueryService.cpp)
target_link_libraries(ServiceTests 
  PRIVATE ${GLOG_LIBRARY}
  PRIVATE ${NEBULA_SERVICE}
  PRIVATE ${GTEST_LIBRARY}
  PRIVATE ${GTEST_MAIN_LIBRARY}
  PRIVATE ${GMOCK_LIBRARY}
  PRIVATE libgrpc++
  PRIVATE libgrpc
  PRIVATE libgpr
  PRIVATE libaddress_sorting
  PRIVATE ${CARES_LIBRARY}
  PRIVATE ${ZLIB_LIBRARY}
  PRIVATE ${XXH_LIBRARY}
  PRIVATE ${JSON_LIBRARY}
  PRIVATE ${PROTOBUF_LIBRARY}
  PRIVATE ${FLATBUFFERS_LIBRARY}
  PRIVATE ${OPENSSL_LIBRARY}
  PRIVATE ${CRYPTO_LIBRARY})
target_compile_options(ServiceTests PRIVATE -Wno-error=unused-parameter)
if(APPLE)
    target_compile_options(ServiceTests PRIVATE -Wno-error=unknown-warning-option)
endif()

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(ServiceTests TEST_LIST ALL)
