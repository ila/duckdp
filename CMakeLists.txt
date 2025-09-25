cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME duckdp)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=0)

# Path to differential privacy library - use the correct path from your workspace
set(DP_THIRD_PARTY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/differential-privacy)
set(DP_ALGORITHMS_LIB ${DP_THIRD_PARTY_DIR}/build/cc/libdp_algorithms.a)

# Include differential privacy headers
include_directories(${DP_THIRD_PARTY_DIR}/cc)

project(${TARGET_NAME})
include_directories(src/include)

# Only include the main extension file, not the parser
set(EXTENSION_SOURCES src/duckdp_extension.cpp)

# Build the C wrapper as a static library
add_library(duckdp_wrapper STATIC
    src/duckdp_c_wrapper.cpp
)

# Make sure the wrapper can find DP headers
target_include_directories(duckdp_wrapper PRIVATE ${DP_THIRD_PARTY_DIR}/cc)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdp_wrapper OpenSSL::SSL OpenSSL::Crypto ${DP_ALGORITHMS_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdp_wrapper OpenSSL::SSL OpenSSL::Crypto ${DP_ALGORITHMS_LIB})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)

install(
  TARGETS duckdp_wrapper
  EXPORT "${DUCKDB_EXPORT_SET}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
