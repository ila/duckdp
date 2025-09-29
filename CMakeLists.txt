cmake_minimum_required(VERSION 3.5)
project(duckdp)

set(TARGET_NAME duckdp)
set(CMAKE_CXX_STANDARD 17)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

# Ensure symbols from the main duckdb binary are exported so the loadable extension
# can resolve RTTI/vtable references (e.g., SimpleNamedParameterFunction typeinfo)
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--export-dynamic")
endif()

include_directories(
        src/include
        third_party/differential-privacy/cc
        third_party/differential-privacy/proto  # <-- add this for correct proto includes
        third_party/differential-privacy/proto/proto  # for generated protobuf headers
        third_party/differential-privacy/proto/proto/accounting  # add accounting subdir for generated headers
        third_party/differential-privacy            # added root for base/ and third_party/ paths
)

# Ensure the extension is built for the correct DuckDB version
set(OVERRIDE_GIT_DESCRIBE "v1.4.0" CACHE STRING "DuckDB version for extension" FORCE)
# Ensure DuckDB's scripts are found correctly
set(DUCKDB_MODULE_BASE_DIR "${CMAKE_SOURCE_DIR}/duckdb" CACHE PATH "DuckDB base dir" FORCE)

add_subdirectory(duckdb)

# Force export-dynamic specifically on the duckdb platform binary target too
if (TARGET duckdb_platform_binary)
    set_property(TARGET duckdb_platform_binary PROPERTY ENABLE_EXPORTS ON)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_link_options(duckdb_platform_binary PRIVATE -Wl,--export-dynamic)
    endif()
endif()

# ALSO apply export-dynamic to the actual DuckDB CLI target (named 'shell', output 'duckdb')
if (TARGET shell)
    set_property(TARGET shell PROPERTY ENABLE_EXPORTS ON)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_link_options(shell PRIVATE -Wl,--export-dynamic)
    endif()
endif()

message(STATUS "OVERRIDE_GIT_DESCRIBE: ${OVERRIDE_GIT_DESCRIBE}")

find_package(absl CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(utf8_range CONFIG REQUIRED)

set(EXTENSION_SOURCES src/duckdp_extension.cpp)

# Minimal differential privacy core sources + cephes (selective, not the full set) -----------------
# (Added because we located inverse_gaussian_cdf in differential-privacy/third_party/cephes)
set(DP_MINIMAL_SOURCES
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/cc/algorithms/numerical-mechanisms.cc
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/cc/algorithms/util.cc
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/cc/algorithms/rand.cc
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/cc/algorithms/distributions.cc
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/cc/algorithms/internal/gaussian-stddev-calculator.cc
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/third_party/cephes/inverse_gaussian_cdf.cc
)
list(APPEND EXTENSION_SOURCES ${DP_MINIMAL_SOURCES})
# Add cephes include path
include_directories(${CMAKE_SOURCE_DIR}/third_party/differential-privacy/third_party)
# -----------------------------------------------------------------------------------------------

# Include generated protobuf sources for differential privacy serialization (LaplaceMechanism, etc.)
file(GLOB_RECURSE DP_PROTO_GEN_SOURCES ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/proto/proto/*.pb.cc)
list(APPEND EXTENSION_SOURCES ${DP_PROTO_GEN_SOURCES})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} "${OVERRIDE_GIT_DESCRIBE}" ${EXTENSION_SOURCES})

# Link absl and protobuf
target_link_libraries(${EXTENSION_NAME} absl::strings absl::status absl::statusor absl::base ${Protobuf_LIBRARIES} OpenSSL::Crypto utf8_range::utf8_range utf8_range::utf8_validity)
target_link_libraries(${LOADABLE_EXTENSION_NAME}
    absl::strings absl::status absl::statusor absl::base absl::hash absl::raw_hash_set absl::flat_hash_map
    absl::log absl::log_internal_check_impl absl::log_internal_conditions absl::log_internal_config absl::log_internal_format absl::log_internal_globals absl::log_internal_log_impl absl::log_internal_message absl::log_internal_nullguard absl::log_internal_nullstream absl::log_internal_proto absl::log_internal_strip absl::log_internal_structured absl::log_internal_structured_proto absl::log_internal_voidify absl::log_severity absl::log_sink absl::log_sink_registry absl::log_streamer absl::log_structured
    ${Protobuf_LIBRARIES} OpenSSL::Crypto utf8_range::utf8_range utf8_range::utf8_validity
)

# Extend loadable extension linkage with absl random libs
set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY INTERFACE_LINK_LIBRARIES "")
target_link_libraries(${LOADABLE_EXTENSION_NAME} absl::random_random absl::random_distributions)

# Include directories for DuckDB and proto headers
target_include_directories(${EXTENSION_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/duckdb/src/include
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/proto
)
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/duckdb/src/include
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy/proto
)

# After targets are defined, ensure root differential-privacy include path is present
# (Needed for includes like third_party/cephes/... and base/status_macros.h)
target_include_directories(${EXTENSION_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy
)
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/third_party/differential-privacy
)

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
