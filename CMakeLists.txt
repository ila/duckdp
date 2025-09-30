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
        ${CMAKE_SOURCE_DIR}/../src/include
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/cc
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/proto
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/proto/proto
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/proto/proto/accounting
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy            # root for base/ and third_party/ paths
)

# Ensure the extension is built for the correct DuckDB version
set(OVERRIDE_GIT_DESCRIBE "v1.4.0" CACHE STRING "DuckDB version for extension" FORCE)

# Normalize DUCKDB_MODULE_BASE_DIR only if it is unset or does not point to a directory containing scripts/append_metadata.cmake
if (NOT DEFINED DUCKDB_MODULE_BASE_DIR OR NOT EXISTS "${DUCKDB_MODULE_BASE_DIR}/scripts/append_metadata.cmake")
    if (EXISTS "${CMAKE_SOURCE_DIR}/scripts/append_metadata.cmake")
        set(DUCKDB_MODULE_BASE_DIR "${CMAKE_SOURCE_DIR}" CACHE PATH "DuckDB base dir" FORCE)
    elseif (EXISTS "${CMAKE_CURRENT_LIST_DIR}/duckdb/scripts/append_metadata.cmake")
        set(DUCKDB_MODULE_BASE_DIR "${CMAKE_CURRENT_LIST_DIR}/duckdb" CACHE PATH "DuckDB base dir" FORCE)
    endif()
endif()
# Collapse accidental duplicate '/duckdb/duckdb/' segments
string(REPLACE "/duckdb/duckdb/" "/duckdb/" _fixed_duckdb_base "${DUCKDB_MODULE_BASE_DIR}/")
# Remove trailing slash added above
string(REGEX REPLACE "/$" "" _fixed_duckdb_base "${_fixed_duckdb_base}")
if (EXISTS "${_fixed_duckdb_base}/scripts/append_metadata.cmake" AND NOT _fixed_duckdb_base STREQUAL "${DUCKDB_MODULE_BASE_DIR}")
    message(STATUS "Normalized DUCKDB_MODULE_BASE_DIR from '${DUCKDB_MODULE_BASE_DIR}' to '${_fixed_duckdb_base}'")
    set(DUCKDB_MODULE_BASE_DIR "${_fixed_duckdb_base}" CACHE PATH "DuckDB base dir" FORCE)
endif()
message(STATUS "Extension resolved DUCKDB_MODULE_BASE_DIR='${DUCKDB_MODULE_BASE_DIR}'")

#add_subdirectory(duckdb)

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

# --- Minimal fallback discovery for OpenSSL and utf8_range (no broad changes) ---
# Allow overrides:
#   OPENSSL_CRYPTO_LIB=/path/to/libcrypto.a
#   UTF8_RANGE_LIB=/path/to/libutf8_range.a
#   UTF8_VALIDITY_LIB=/path/to/libutf8_validity.a
set(_EXT_ROOT "${CMAKE_CURRENT_LIST_DIR}")
set(_VCPKG_LIB_DIR "${_EXT_ROOT}/vcpkg_installed/x64-linux/lib")
set(_VCPKG_INC_DIR "${_EXT_ROOT}/vcpkg_installed/x64-linux/include")

if (NOT TARGET OpenSSL::Crypto)
    set(_openssl_candidate "${_VCPKG_LIB_DIR}/libcrypto.a")
    if (DEFINED OPENSSL_CRYPTO_LIB)
        set(_openssl_candidate "${OPENSSL_CRYPTO_LIB}")
    endif()
    if (EXISTS "${_openssl_candidate}")
        add_library(OpenSSL::Crypto STATIC IMPORTED)
        set_target_properties(OpenSSL::Crypto PROPERTIES IMPORTED_LOCATION "${_openssl_candidate}" INTERFACE_INCLUDE_DIRECTORIES "${_VCPKG_INC_DIR}")
        message(STATUS "Using fallback OpenSSL::Crypto at ${_openssl_candidate}")
    else()
        message(FATAL_ERROR "Could not locate libcrypto.a. Set OPENSSL_CRYPTO_LIB to its path.")
    endif()
endif()

# utf8_range fallback targets
if (NOT TARGET utf8_range::utf8_range)
    set(_utf8_range_candidate "${_VCPKG_LIB_DIR}/libutf8_range.a")
    if (DEFINED UTF8_RANGE_LIB)
        set(_utf8_range_candidate "${UTF8_RANGE_LIB}")
    endif()
    if (EXISTS "${_utf8_range_candidate}")
        add_library(utf8_range::utf8_range STATIC IMPORTED)
        set_target_properties(utf8_range::utf8_range PROPERTIES IMPORTED_LOCATION "${_utf8_range_candidate}" INTERFACE_INCLUDE_DIRECTORIES "${_VCPKG_INC_DIR}")
        message(STATUS "Using fallback utf8_range at ${_utf8_range_candidate}")
    endif()
endif()
if (NOT TARGET utf8_range::utf8_validity)
    set(_utf8_validity_candidate "${_VCPKG_LIB_DIR}/libutf8_validity.a")
    if (DEFINED UTF8_VALIDITY_LIB)
        set(_utf8_validity_candidate "${UTF8_VALIDITY_LIB}")
    endif()
    if (EXISTS "${_utf8_validity_candidate}")
        add_library(utf8_range::utf8_validity STATIC IMPORTED)
        set_target_properties(utf8_range::utf8_validity PROPERTIES IMPORTED_LOCATION "${_utf8_validity_candidate}" INTERFACE_INCLUDE_DIRECTORIES "${_VCPKG_INC_DIR}")
        message(STATUS "Using fallback utf8_validity at ${_utf8_validity_candidate}")
    endif()
endif()
# -------------------------------------------------------------------------------

find_package(absl CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
#find_package(OpenSSL REQUIRED)
#find_package(utf8_range CONFIG REQUIRED)

set(EXTENSION_SOURCES src/duckdp_extension.cpp)

# Minimal differential privacy core sources + cephes (selective, not the full set) -----------------
# (Added because we located inverse_gaussian_cdf in differential-privacy/third_party/cephes)
set(DP_MINIMAL_SOURCES
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/cc/algorithms/numerical-mechanisms.cc
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/cc/algorithms/util.cc
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/cc/algorithms/rand.cc
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/cc/algorithms/distributions.cc
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/cc/algorithms/internal/gaussian-stddev-calculator.cc
        ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/third_party/cephes/inverse_gaussian_cdf.cc
)
list(APPEND EXTENSION_SOURCES ${DP_MINIMAL_SOURCES})
# Add cephes include path
include_directories(${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/third_party)
# -----------------------------------------------------------------------------------------------

# Include generated protobuf sources for differential privacy serialization (LaplaceMechanism, etc.)
file(GLOB_RECURSE DP_PROTO_GEN_SOURCES ${CMAKE_SOURCE_DIR}/../third_party/differential-privacy/proto/proto/*.pb.cc)
list(APPEND EXTENSION_SOURCES ${DP_PROTO_GEN_SOURCES})

#include(${DUCKDB_MODULE_BASE_DIR}/extension/extension_config.cmake)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link absl and protobuf
target_link_libraries(${EXTENSION_NAME} absl::strings absl::status absl::statusor absl::base protobuf::libprotobuf OpenSSL::Crypto utf8_range::utf8_range utf8_range::utf8_validity absl::random_random absl::random_distributions)

target_link_libraries(${LOADABLE_EXTENSION_NAME} absl::strings absl::status absl::statusor absl::base absl::hash absl::raw_hash_set absl::flat_hash_map
        absl::log absl::log_internal_check_impl absl::log_internal_conditions absl::log_internal_config absl::log_internal_format absl::log_internal_globals absl::log_internal_log_impl absl::log_internal_message absl::log_internal_nullguard absl::log_internal_nullstream absl::log_internal_proto absl::log_internal_strip absl::log_internal_structured absl::log_internal_structured_proto absl::log_internal_voidify absl::log_severity absl::log_sink absl::log_sink_registry absl::log_streamer absl::log_structured
        protobuf::libprotobuf OpenSSL::Crypto utf8_range::utf8_range utf8_range::utf8_validity absl::random_random absl::random_distributions
)

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
