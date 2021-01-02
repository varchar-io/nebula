find_package(Threads REQUIRED)

# not sure why we only add this for APPLE only.
include(ExternalProject)
ExternalProject_Add(protobuf
    PREFIX protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v3.14.0
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(protobuf SOURCE_DIR)
add_custom_target(mprotobuf ALL
      COMMAND git submodule update --init --recursive && ./autogen.sh && ./configure --prefix=${SOURCE_DIR} && make install -j8
      WORKING_DIRECTORY ${SOURCE_DIR}
      DEPENDS protobuf)

set(PROTOBUF_INCLUDE_DIRS ${SOURCE_DIR}/include)
set(PROTOBUF_LIBRARY_PATH ${SOURCE_DIR}/lib/libprotobuf.a)
set(PROTOBUF_LIBRARY libpb)
add_library(${PROTOBUF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${PROTOBUF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${PROTOBUF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${PROTOBUF_INCLUDE_DIRS}")
add_dependencies(${PROTOBUF_LIBRARY} mprotobuf)

# set PROTO compiler
SET(PROTO_COMPILER ${SOURCE_DIR}/bin/protoc)
