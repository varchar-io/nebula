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
SET(GEN_DIR "${SERVICE_DIR}/gen/helloworld")
file(MAKE_DIRECTORY ${GEN_DIR})

get_filename_component(hw_proto "${SERVICE_DIR}/protos/helloworld.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources from proto file
SET(HW_DIR "${SERVICE_DIR}/helloworld")
set(hw_proto_srcs "${GEN_DIR}/helloworld.pb.cc")
set(hw_proto_hdrs "${GEN_DIR}/helloworld.pb.h")
set(hw_grpc_srcs "${GEN_DIR}/helloworld.grpc.pb.cc")
set(hw_grpc_hdrs "${GEN_DIR}/helloworld.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${PROTO_COMPILER}
      ARGS --grpc_out "${GEN_DIR}"
        --cpp_out "${GEN_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
        "${hw_proto}"
      DEPENDS ${hw_proto})

# Include generated *.pb.h files
include_directories("${GEN_DIR}")

# Targets greeter_[async_](client|server)
foreach(_target
  greeter_client greeter_server)
  add_executable(${_target} "${HW_DIR}/${_target}.cpp"
    ${hw_proto_srcs}
    ${hw_grpc_srcs})
  
  # NOTE that - on linux, GCC behaves wired, the order of the dependencies matter
  # which means, libgrpc++ depends on libgrpc, and likewise, libgrpc depends on libgpr and address_sorting
  # if we messed up the order, the link will report huge amount of errors like undefined referneces.
  target_link_libraries(${_target}
    PRIVATE libgrpc++
    PRIVATE libgrpc
    PRIVATE libgpr
    PRIVATE libaddress_sorting
    PRIVATE ${GFLAGS_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE ${CARES_LIBRARY}
    PRIVATE ${ZLIB_LIBRARY}
    PRIVATE ${PROTOBUF_LIBRARY})
    # disalbe warning into errors for due to these generated files
    target_compile_options(${_target} PRIVATE -Wno-error=unused-parameter)
endforeach()

# build a docker target for greeter server
get_filename_component(docker_gs "${SERVICE_DIR}/helloworld/greeter_server.Dockerfile" ABSOLUTE)
add_custom_target(gsdocker
      ALL COMMAND docker build -t nebula/greeter_server -f ${docker_gs} .
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      DEPENDS greeter_server)

# build client library for helloworld
add_custom_target(hw_web_client
      ALL COMMAND ${PROTO_COMPILER} 
        --js_out=import_style=commonjs:"${GEN_DIR}"
        -I "${hw_proto_path}"
        --grpc-web_out=import_style=commonjs,mode=grpcwebtext:"${GEN_DIR}"
        --plugin=protoc-gen-grpc-web=${GRPC_WEB_PLUGIN}
        "${hw_proto}"
      WORKING_DIRECTORY ${SERVICE_DIR}
      DEPENDS ${hw_proto})

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
get_filename_component(docker_envoy "${SERVICE_DIR}/http/envoy.Dockerfile" ABSOLUTE)
add_custom_target(envoydocker
      ALL COMMAND docker build -t nebula/envoy -f ${docker_envoy} .
      WORKING_DIRECTORY ${SERVICE_DIR}/http
      DEPENDS ${docker_envoy})


# we will also run a simple web server to serve the page which hosts our client.js logic
# inside the client.js logic, it will call into 8080 port for data request through envoy proxy
# envoy proxy will contact 9090 port where is the real server for response.
# to run the web server, first we pack all js file into a single one
# here we are using webpack:
# $ npm install (based on package.json - can be reused for all service)
# $ npx webpack client.js

