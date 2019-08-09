if(APPLE)
    set(dcdir /usr/local/Cellar/double-conversion/3.1.5)
    set(DC_INCLUDE_DIRS ${dcdir}/include)
    set(DC_LIBRARY_PATH ${dcdir}/lib/libdouble-conversion.a)
else()
    # define double-conversion
    set(DC_INCLUDE_DIRS /usr/local/include)
    set(DC_LIBRARY_PATH /usr/local/lib/libdouble-conversion.a)
endif()

set(DC_LIBRARY doublec)
add_library(${DC_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${DC_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${DC_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${DC_INCLUDE_DIRS}")