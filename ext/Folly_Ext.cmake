# folly was installed manually through brew install folly
# we should consider add it as an external target
# add include folder here after manual installation
# TODO(cao) - conan may be worth looking for package management 
# so that we don't manually manage dependencies like folly 
# ref link - https://github.com/conan-io/examples/tree/master/libraries/folly/basic
# step - https://blog.conan.io/2018/12/03/Using-Facebook-Folly-with-Conan.html

# define boost
set(boostDir /usr/local/Cellar/boost/1.69.0)
set(BOOST_INCLUDE_DIRS ${boostDir}/include)
# boost_system
set(BOOST_SYSTEM_PATH ${boostDir}/lib/libboost_system.a)
set(BOOST_SYSTEM BOOSTSYS)
add_library(${BOOST_SYSTEM} UNKNOWN IMPORTED)
set_target_properties(${BOOST_SYSTEM} PROPERTIES
    "IMPORTED_LOCATION" "${BOOST_SYSTEM_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${BOOST_INCLUDE_DIRS}")
include_directories(include ${BOOST_INCLUDE_DIRS})

# folly depends on double-conversion
set(dcdir /usr/local/Cellar/double-conversion/3.1.4)
set(DC_INCLUDE_DIRS ${dcdir}/include)
set(DC_LIBRARY_PATH ${dcdir}/lib/libdouble-conversion.a)
set(DC_LIBRARY DC)
add_library(${DC_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${DC_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${DC_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${DC_INCLUDE_DIRS}")
include_directories(include ${DC_INCLUDE_DIRS})

# define libevent
set(ledir /usr/local/Cellar/libevent/2.1.8)
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
set(FollyDir /usr/local/Cellar/folly/2019.03.18.00_2)
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