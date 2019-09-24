# Adding kafka support to read streaming data
find_package(Threads REQUIRED)

# http://roaringbitmap.org/
# by default, roaring is providng dynamic lib for linking
# however I changed it to build static here - we may want to adjust in final deployment
include(ExternalProject)
SET(KAFKA_OPTS
  -DCMAKE_BUILD_TYPE=Release
  -DRDKAFKA_BUILD_STATIC=1
  -DENABLE_LZ4_EXT=OFF
  -DWITH_SSL=OFF)
ExternalProject_Add(Kafka
  PREFIX kafka
  GIT_REPOSITORY https://github.com/edenhill/librdkafka.git
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  CMAKE_ARGS ${KAFKA_OPTS}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(Kafka SOURCE_DIR)
ExternalProject_Get_Property(Kafka BINARY_DIR)

# kafka C
set(KC_INCLUDE_DIRS ${SOURCE_DIR}/src)
file(MAKE_DIRECTORY ${KC_INCLUDE_DIRS})
set(KC_LIBRARY_PATH ${BINARY_DIR}/src/librdkafka.a)
set(KC_LIBRARY libkafkac)
add_library(${KC_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${KC_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${KC_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${KC_INCLUDE_DIRS}")

# kafka C++
set(KAFKA_INCLUDE_DIRS ${SOURCE_DIR}/src-cpp)
file(MAKE_DIRECTORY ${KAFKA_INCLUDE_DIRS})
set(KAFKA_LIBRARY_PATH ${BINARY_DIR}/src-cpp/librdkafka++.a)
set(KAFKA_LIBRARY libkafka)
add_library(${KAFKA_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${KAFKA_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${KAFKA_LIBRARY_PATH}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${KAFKA_INCLUDE_DIRS}")

target_link_libraries(${KAFKA_LIBRARY} 
  INTERFACE ${KC_LIBRARY}
  INTERFACE ${ZSTD_LIBRARY})

add_dependencies(${KAFKA_LIBRARY} kafka)
