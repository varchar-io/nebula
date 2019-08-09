if(APPLE)
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

    # install protobuf, please go to the protobuf build folder
    # and manually run the install there with sudo. 
    # since it requires access, let's not turn it on in build script
    # add_custom_target(install_protobuf
    #       ALL COMMAND make install
    #       WORKING_DIRECTORY ${BINARY_DIR}
    #       DEPENDS ${PROTOBUF_LIBRARY})

    # set PROTO compiler
    SET(PROTO_COMPILER ${BINARY_DIR}/protoc)

else()

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
    
endif()