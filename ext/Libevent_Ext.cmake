if(APPLE)
    # define libevent
    set(ledir /usr/local/opt/libevent)
    set(EVENT_INCLUDE_DIRS ${ledir}/include)
    set(EVENT_LIBRARY_PATH ${ledir}/lib/libevent.a)
    set(EVENT_CORE_PATH ${ledir}/lib/libevent_core.a)
else()
    set(EVENT_INCLUDE_DIRS /usr/local/include)
    set(EVENT_LIBRARY_PATH /usr/local/lib/libevent.a)
    set(EVENT_CORE_PATH /usr/local/lib/libevent_core.a)
endif()

set(EVENT_LIBRARY libevent)
add_library(${EVENT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${EVENT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${EVENT_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${EVENT_INCLUDE_DIRS}")

set(EVENT_CORE libevent_core)
add_library(${EVENT_CORE} UNKNOWN IMPORTED)
set_target_properties(${EVENT_CORE} PROPERTIES
    "IMPORTED_LOCATION" "${EVENT_CORE_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${EVENT_INCLUDE_DIRS}")
