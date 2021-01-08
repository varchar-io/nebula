# OPENSSL has to be available for find_package
# it's not only used by nebula but also required by other depedency build
# NOTE: some platform has system provided openssl
# and this find_package will use that instead
# We require build script install customized openssl in ${OPENSSL_ROOT_DIR}
# set(OpenSSL_USE_STATIC_LIBS ON)
# find_package(OpenSSL REQUIRED)

set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT_DIR}/include)
set(SSL_LIB_PATH ${OPENSSL_ROOT_DIR}/lib/libssl.a)
set(OPENSSL_LIBRARY ssl)
add_library(${OPENSSL_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${OPENSSL_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${SSL_LIB_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${OPENSSL_INCLUDE_DIR}")

set(CRYPTO_LIBRARY crypto)
set(CRYPTO_LIB_PATH ${OPENSSL_ROOT_DIR}/lib/libcrypto.a)
add_library(${CRYPTO_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${CRYPTO_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${CRYPTO_LIB_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${OPENSSL_INCLUDE_DIR}")
