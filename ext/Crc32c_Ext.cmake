find_package(Threads REQUIRED)

include(ExternalProject)

SET(CRC32C_OPTS
    -DCRC32C_BUILD_TESTS=OFF
    -DCRC32C_BUILD_BENCHMARKS=OFF)
ExternalProject_Add(crc32c
    PREFIX crc32c
    GIT_REPOSITORY https://github.com/google/crc32c.git
    GIT_TAG 1.1.1
    CMAKE_ARGS ${CRC32C_OPTS}
    UPDATE_COMMAND ""
    INSTALL_DIR ${NEBULA_INSTALL}
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)