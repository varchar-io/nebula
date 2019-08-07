# folly was installed manually through brew install folly
# we should consider add it as an external target
# add include folder here after manual installation
# TODO(cao) - conan may be worth looking for package management 
# so that we don't manually manage dependencies like folly 
# ref link - https://github.com/conan-io/examples/tree/master/libraries/folly/basic
# step - https://blog.conan.io/2018/12/03/Using-Facebook-Folly-with-Conan.html


# installing Folly on linux (Ubuntu is not easy)
# here are some tips
# 1. download a release version of folly to build and make install 
#   https://github.com/facebook/folly/releases
# 2. a lot dependency we need to rebuild and install
# 2.1 reinstall ZSTD https://github.com/facebook/zstd
#     "sudo make install" at root
# 2.2 install libevent from 
# have to specify gcc7 to not miss libautomatic and build type as release without
# 2.3 install GCC7 "sudo apt install g++-7 -y"
# 3. Lots of prerequisites to install
# sudo apt-get install \
# g++ \
# automake \
# autoconf \
# autoconf-archive \
# libtool \
# libboost-all-dev \
# libevent-dev \
# libdouble-conversion-dev \
# libgoogle-glog-dev \
# libgflags-dev \
# liblz4-dev \
# liblzma-dev \
# libsnappy-dev \
# make \
# zlib1g-dev \
# binutils-dev \
# libjemalloc-dev \
# libssl-dev
# libunwind8-dev \
# libelf-dev \
# libdwarf-dev
# libiberty-dev
# 4. finally call cmake and make in build folder (this CMAKE build type is critical to make it work)
# folly/build> "cmake .. -DCMAKE_CXX_COMPILER=/usr/bin/g++-7 -DCMAKE_CC_COMPILER=/usr/bin/gcc-7 -DCMAKE_BUILD_TYPE=Release"
# folly/build> make && sudo make install
# To ensure linker will use the same version for all dependecies, please do the same command to install these before install folly
# * GFLAGS
# * GLOG
# 5. We need to install open SSL to pass the build https://github.com/openssl/openssl.git
# $ ./config
# $ make
# $ make test
# $ make install
# 6. TODO(cao) - should we automate this? otherwise every new DEV will go through the installation steps for folly.

if(APPLE)
    # folly depends on double-conversion
    set(dcdir /usr/local/Cellar/double-conversion/3.1.5)
    set(DC_INCLUDE_DIRS ${dcdir}/include)
    set(DC_LIBRARY_PATH ${dcdir}/lib/libdouble-conversion.a)
    set(DC_LIBRARY DC)
    add_library(${DC_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${DC_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${DC_LIBRARY_PATH}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${DC_INCLUDE_DIRS}")
    include_directories(include ${DC_INCLUDE_DIRS})

    # define libevent
    set(ledir /usr/local/Cellar/libevent/2.1.11)
    set(LE_INCLUDE_DIRS ${ledir}/include)
    set(LE_LIBRARY_PATH ${ledir}/lib/libevent.a)
    set(LE_CORE_PATH ${ledir}/lib/libevent_core.a)
    set(LE_LIBRARY LE)
    set(LE_CORE LECORE)
    add_library(${LE_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${LE_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${LE_LIBRARY_PATH}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${LE_INCLUDE_DIRS}")
    add_library(${LE_CORE} UNKNOWN IMPORTED)
    set_target_properties(${LE_CORE} PROPERTIES
        "IMPORTED_LOCATION" "${LE_CORE_PATH}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${LE_INCLUDE_DIRS}")
    include_directories(include ${LE_INCLUDE_DIRS})

    # define folly
    set(FollyDir /usr/local/Cellar/folly/2019.08.05.00)
    set(FOLLY_INCLUDE_DIRS ${FollyDir}/include)
    set(FOLLY_LIBRARY_PATH ${FollyDir}/lib/libfolly.a)
    set(FOLLY_LIBRARY folly)
    add_library(${FOLLY_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${FOLLY_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${FOLLY_LIBRARY_PATH}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${FOLLY_INCLUDE_DIRS}")
    include_directories(include ${FOLLY_INCLUDE_DIRS})

    # folly depends on dc
    target_link_libraries(${FOLLY_LIBRARY} 
        INTERFACE ${DC_LIBRARY} 
        INTERFACE ${LE_LIBRARY})

    # add installed arrow here
    # NOTE: we don't want conda to pollute our includes
    # so copy array includes to another one 
    # cp -r /usr/local/conda/include/arrow /usr/local/arrow/include
    # set(ARROW_INCLUDE_DIRS /usr/local/arrow/include)
    # set(ARROW_LIBRARY_PATH /usr/local/conda/lib/libarrow.dylib)
    # set(ARROW_LIBRARY arrow)
    # add_library(${ARROW_LIBRARY} UNKNOWN IMPORTED)
    # set_target_properties(${ARROW_LIBRARY} PROPERTIES
    #     "IMPORTED_LOCATION" "${ARROW_LIBRARY_PATH}"
    #     "INTERFACE_INCLUDE_DIRECTORIES" "${ARROW_INCLUDE_DIRS}")
    # include_directories(include ${ARROW_INCLUDE_DIRS})
else()
    # define folly - we may not need this since it's installed in system path already
    set(FOLLY_INCLUDE_DIRS /usr/local/include)
    set(FOLLY_LIBRARY_PATH /usr/local/lib/libfolly.a)
    set(FOLLY_LIBRARY folly)
    add_library(${FOLLY_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${FOLLY_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${FOLLY_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${FOLLY_INCLUDE_DIRS}")
    
    # define double-conversion
    set(DC_INCLUDE_DIRS /usr/local/include)
    set(DC_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libdouble-conversion.a)
    set(DC_LIBRARY doublec)
    add_library(${DC_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${DC_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${DC_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${DC_INCLUDE_DIRS}")
        
    # define libiberty
    set(IBERTY_INCLUDE_DIRS /usr/local/include)
    set(IBERTY_LIBRARY_PATH /usr/lib/x86_64-linux-gnu/libiberty.a)
    set(IBERTY_LIBRARY iberty)
    add_library(${IBERTY_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${IBERTY_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${IBERTY_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${IBERTY_INCLUDE_DIRS}")
    
    # define openssl - libssl and libcrypto
    set(CRYPTO_INCLUDE_DIRS /usr/local/include)
    set(CRYPTO_LIBRARY_PATH /usr/local/lib/libcrypto.a)
    set(CRYPTO_LIBRARY crypto)
    add_library(${CRYPTO_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${CRYPTO_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${CRYPTO_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${CRYPTO_INCLUDE_DIRS}")
    
    
    set(EVENT_INCLUDE_DIRS /usr/local/include)
    set(EVENT_LIBRARY_PATH /usr/local/lib/libevent.a)
    set(EVENT_LIBRARY libevent)
    add_library(${EVENT_LIBRARY} UNKNOWN IMPORTED)
    set_target_properties(${EVENT_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${EVENT_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${EVENT_INCLUDE_DIRS}")
    
    target_link_libraries(${FOLLY_LIBRARY} 
        INTERFACE ${IBERTY_LIBRARY}
        INTERFACE ${OPENSSL_LIBRARY}
        INTERFACE ${CRYPTO_LIBRARY}
        INTERFACE ${DC_LIBRARY}
        INTERFACE ${EVENT_LIBRARY}
        INTERFACE ${BOOST_CONTEXT_LIBRARY}
        INTERFACE ${BOOST_PROGRAM_OPTIONS_LIBRARY}
        INTERFACE ${BOOST_REGEX_LIBRARY}
        INTERFACE ${BOOST_FS_LIBRARY})
endif()