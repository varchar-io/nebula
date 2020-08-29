if(APPLE)
    # open ssl
    set(OPENSSL_ROOT ${CELLAR_ROOT}/openssl@1.1/1.1.1g)
    set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT}/include)
    set(OPENSSL_LIBRARY_PATH ${OPENSSL_ROOT}/lib/libssl.a)
    set(OPENSSL_LIBRARY openssl)
    add_library(${OPENSSL_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${OPENSSL_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${OPENSSL_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${OPENSSL_INCLUDE_DIR}")

    # os provided "sudo locate libcrypto.a"
    set(CRYPTO_LIBRARY_PATH ${OPENSSL_ROOT}/lib/libcrypto.a)
    set(CRYPTO_LIBRARY crypto)
    add_library(${CRYPTO_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${CRYPTO_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${CRYPTO_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${OPENSSL_INCLUDE_DIR}")

else()
    set(OPENSSL_ROOT /usr/local/ssl)
    set(OPENSSL_INCLUDE_DIR /usr/local/include)
    set(OPENSSL_LIBRARY_PATH /usr/local/lib/libssl.a)
    set(OPENSSL_LIBRARY openssl)
    add_library(${OPENSSL_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${OPENSSL_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${OPENSSL_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${OPENSSL_INCLUDE_DIR}")

    # os provided "sudo locate libcrypto.a"
    set(CRYPTO_INCLUDE_DIRS /usr/local/include)
    set(CRYPTO_LIBRARY_PATH /usr/local/lib/libcrypto.a)
    set(CRYPTO_LIBRARY crypto)
    add_library(${CRYPTO_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${CRYPTO_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${CRYPTO_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${CRYPTO_INCLUDE_DIRS}")
    
    # define libiberty
    set(IBERTY_INCLUDE_DIRS /usr/local/include)
    set(IBERTY_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libiberty.a)
    set(IBERTY_LIBRARY iberty)
    add_library(${IBERTY_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${IBERTY_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${IBERTY_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${IBERTY_INCLUDE_DIRS}")

endif()