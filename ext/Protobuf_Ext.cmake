find_package(Threads REQUIRED)

# not sure why we only add this for APPLE only.
include(ExternalProject)
ExternalProject_Add(protobuf
    PREFIX protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v3.19.2
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(protobuf SOURCE_DIR)
set(INST_DIR /usr/local)

set(CMD_UPDATE_CACHE ldconfig)
if(APPLE)
set(CMD_UPDATE_CACHE update_dyld_shared_cache)
endif()

add_custom_target(mprotobuf ALL
    COMMAND sudo git submodule update --init --recursive && sudo ./autogen.sh && sudo ./configure --prefix=${INST_DIR} && sudo make install -j8 && sudo ${CMD_UPDATE_CACHE}
    WORKING_DIRECTORY ${SOURCE_DIR}
    DEPENDS protobuf)

set(PROTOBUF_INCLUDE_DIRS ${INST_DIR}/include)
file(MAKE_DIRECTORY ${PROTOBUF_INCLUDE_DIRS})
set(PROTOBUF_LIBRARY_PATH ${INST_DIR}/lib/libprotobuf.a)
set(PROTOBUF_LIBRARY libpb)
add_library(${PROTOBUF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${PROTOBUF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${PROTOBUF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${PROTOBUF_INCLUDE_DIRS}")
add_dependencies(${PROTOBUF_LIBRARY} mprotobuf)

# set PROTO compiler
SET(PROTO_COMPILER ${INST_DIR}/bin/protoc)

# alias used by some components in grpc
SET(Protobuf_PROTOC_LIBRARY ${PROTO_COMPILER})
SET(Protobuf_INCLUDE_DIR ${PROTOBUF_INCLUDE_DIRS})
