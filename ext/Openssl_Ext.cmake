set(OpenSSL_USE_STATIC_LIBS ON)
find_package(OpenSSL REQUIRED)


if(APPLE)
    set(OPENSSL_SSL_LIBRARY "/usr/local/opt/openssl/lib/libssl.a")
endif()


# TODO(cao): this is a hack to link static ssl
# due to not working option `OPENSSL_USE_STATIC_LIBS`
get_filename_component(LIBSSL_DIR ${OPENSSL_SSL_LIBRARY} DIRECTORY)
set(OPENSSL_LIBRARY ssl)
add_library(${OPENSSL_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${OPENSSL_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${LIBSSL_DIR}/libssl.a"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${OPENSSL_INCLUDE_DIR}")

# os provided "sudo locate libcrypto.a"
set(CRYPTO_LIBRARY crypto)
add_library(${CRYPTO_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${CRYPTO_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${LIBSSL_DIR}/libcrypto.a"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${OPENSSL_INCLUDE_DIR}")
