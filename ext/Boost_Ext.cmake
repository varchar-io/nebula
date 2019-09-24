# INSTALL boost specific version, 1.69 is needed due to compability requirement such as parquet
# 0. tmp folder or build folder
# 1. wget https://dl.bintray.com/boostorg/release/1.69.0/source/boost_1_69_0.tar.gz
# 2. tar -zxvf boost_1_69_0.tar.gz
# 3. cd boost_1_69_0/
# 4. sudo ./bootstrap.sh --prefix=/usr/local
# 5. sudo ./b2 stage threading=multi -j36
# 6. sudo ./b2 install
# define boost
set(boostDir /usr/local)
set(Boost_INCLUDE_DIRS ${boostDir}/include)
set(BOOST_CONTEXT_PATH ${boostDir}/lib/libboost_context.a)
# boost_context

set(BOOST_CONTEXT_LIBRARY boost_context)
add_library(${BOOST_CONTEXT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${BOOST_CONTEXT_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${BOOST_CONTEXT_PATH}")

# boost_system
set(BOOST_SYSTEM_PATH ${boostDir}/lib/libboost_system.a)
set(BOOST_SYSTEM_LIBRARY boost_system)
add_library(${BOOST_SYSTEM_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${BOOST_SYSTEM_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${BOOST_SYSTEM_PATH}")

# boost_regex
set(BOOST_REGEX_PATH ${boostDir}/lib/libboost_regex.a)
set(BOOST_REGEX_LIBRARY boost_regex)
add_library(${BOOST_REGEX_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${BOOST_REGEX_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${BOOST_REGEX_PATH}")

# boost_filesystem
set(BOOST_FIlESYSTEM_PATH ${boostDir}/lib/libboost_filesystem.a)
set(BOOST_FS_LIBRARY boost_filesystem)
add_library(${BOOST_FS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${BOOST_FS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${BOOST_FIlESYSTEM_PATH}")

# boost_program_options
set(BOOST_PROGRAM_OPTIONS_PATH ${boostDir}/lib/libboost_program_options.a)
set(BOOST_PROGRAM_OPTIONS_LIBRARY boost_program_options)
add_library(${BOOST_PROGRAM_OPTIONS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${BOOST_PROGRAM_OPTIONS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${BOOST_PROGRAM_OPTIONS_PATH}")