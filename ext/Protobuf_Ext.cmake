find_package(Threads REQUIRED)
find_package(absl REQUIRED)

# not sure why we only add this for APPLE only.
include(ExternalProject)
ExternalProject_Add(protobuf
    PREFIX protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v29.3
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(protobuf SOURCE_DIR)

add_custom_target(mprotobuf ALL
    COMMAND sudo git submodule update --init --recursive && sudo cmake . -DCMAKE_CXX_STANDARD=17 -Dprotobuf_ABSL_PROVIDER=package -Dprotobuf_LOCAL_DEPENDENCIES_ONLY=ON -Dprotobuf_FETCH_DEPENDENCIES=OFF -Dprotobuf_BUILD_SHARED_LIBS=OFF && sudo cmake --build . && sudo cmake --install .
    WORKING_DIRECTORY ${SOURCE_DIR}
    DEPENDS protobuf)

set(PROTOBUF_INCLUDE_DIRS /usr/local/include)
set(PROTOBUF_LIBRARY_PATH /usr/local/lib/libprotobuf.a)
set(PROTOBUF_LIBRARY libpb)
add_library(${PROTOBUF_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${PROTOBUF_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${PROTOBUF_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${PROTOBUF_INCLUDE_DIRS}")
add_dependencies(${PROTOBUF_LIBRARY} mprotobuf)
target_link_libraries(${PROTOBUF_LIBRARY}
    INTERFACE absl::bad_any_cast_impl
    INTERFACE absl::bad_optional_access
    INTERFACE absl::bad_variant_access
    INTERFACE absl::base
    INTERFACE absl::city
    INTERFACE absl::civil_time
    INTERFACE absl::cord
    INTERFACE absl::cord_internal
    INTERFACE absl::cordz_functions
    INTERFACE absl::cordz_handle
    INTERFACE absl::cordz_info
    INTERFACE absl::cordz_sample_token
    INTERFACE absl::crc32c
    INTERFACE absl::crc_cord_state
    INTERFACE absl::crc_cpu_detect
    INTERFACE absl::crc_internal
    INTERFACE absl::debugging_internal
    INTERFACE absl::decode_rust_punycode
    INTERFACE absl::demangle_internal
    INTERFACE absl::demangle_rust
    INTERFACE absl::die_if_null
    INTERFACE absl::examine_stack
    INTERFACE absl::exponential_biased
    INTERFACE absl::failure_signal_handler
    INTERFACE absl::flags_commandlineflag
    INTERFACE absl::flags_commandlineflag_internal
    INTERFACE absl::flags_config
    INTERFACE absl::flags_internal
    INTERFACE absl::flags_marshalling
    INTERFACE absl::flags_parse
    INTERFACE absl::flags_private_handle_accessor
    INTERFACE absl::flags_program_name
    INTERFACE absl::flags_reflection
    INTERFACE absl::flags_usage
    INTERFACE absl::flags_usage_internal
    INTERFACE absl::graphcycles_internal
    INTERFACE absl::hash
    INTERFACE absl::hashtablez_sampler
    INTERFACE absl::int128
    INTERFACE absl::kernel_timeout_internal
    INTERFACE absl::leak_check
    INTERFACE absl::log_entry
    INTERFACE absl::log_flags
    INTERFACE absl::log_globals
    INTERFACE absl::log_initialize
    INTERFACE absl::log_internal_check_op
    INTERFACE absl::log_internal_conditions
    INTERFACE absl::log_internal_fnmatch
    INTERFACE absl::log_internal_format
    INTERFACE absl::log_internal_globals
    INTERFACE absl::log_internal_log_sink_set
    INTERFACE absl::log_internal_message
    INTERFACE absl::log_internal_nullguard
    INTERFACE absl::log_internal_proto
    INTERFACE absl::log_severity
    INTERFACE absl::log_sink
    INTERFACE absl::low_level_hash
    INTERFACE absl::malloc_internal
    INTERFACE absl::periodic_sampler
    INTERFACE absl::poison
    INTERFACE absl::random_distributions
    INTERFACE absl::random_internal_distribution_test_util
    INTERFACE absl::random_internal_platform
    INTERFACE absl::random_internal_pool_urbg
    INTERFACE absl::random_internal_randen
    INTERFACE absl::random_internal_randen_hwaes
    INTERFACE absl::random_internal_randen_hwaes_impl
    INTERFACE absl::random_internal_randen_slow
    INTERFACE absl::random_internal_seed_material
    INTERFACE absl::random_seed_gen_exception
    INTERFACE absl::random_seed_sequences
    INTERFACE absl::raw_hash_set
    INTERFACE absl::raw_logging_internal
    INTERFACE absl::scoped_set_env
    INTERFACE absl::spinlock_wait
    INTERFACE absl::stacktrace
    INTERFACE absl::status
    INTERFACE absl::statusor
    INTERFACE absl::str_format_internal
    INTERFACE absl::strerror
    INTERFACE absl::string_view
    INTERFACE absl::strings
    INTERFACE absl::strings_internal
    INTERFACE absl::symbolize
    INTERFACE absl::synchronization
    INTERFACE absl::throw_delegate
    INTERFACE absl::time
    INTERFACE absl::time_zone
    INTERFACE absl::utf8_for_code_point
    INTERFACE absl::vlog_config_internal
    INTERFACE utf8_range::utf8_range
    INTERFACE utf8_range::utf8_validity)

# alias used by some components in grpc
set(PROTO_COMPILER /usr/local/bin/protoc)
