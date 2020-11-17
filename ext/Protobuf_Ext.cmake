# Follow dev.md to manual installs protobuf
set(PROTOBUF_INCLUDE_DIRS /usr/local/include)
set(PROTOBUF_LIBRARY_PATH /usr/local/lib/libprotobuf.a)
set(PROTOBUF_LIBRARY libpb)
add_library(${PROTOBUF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${PROTOBUF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${PROTOBUF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${PROTOBUF_INCLUDE_DIRS}")
add_dependencies(${PROTOBUF_LIBRARY} protobuf)

# set PROTO compiler
SET(PROTO_COMPILER /usr/local/bin/protoc)
