PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Attempt auto-detection of a vcpkg toolchain file.
VCPKG_TOOLCHAIN_PATH := /home/ila/vcpkg/scripts/buildsystems/vcpkg.cmake

# (User may still override by passing VCPKG_TOOLCHAIN_PATH=... on the make command line)

# Configuration of extension
EXT_NAME=duckdp
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Expose chosen toolchain path in an info target (optional)
.PHONY: print-toolchain
print-toolchain:
	@echo Using VCPKG_TOOLCHAIN_PATH='${VCPKG_TOOLCHAIN_PATH}'

# Include the Makefile from extension-ci-tools (consumes VCPKG_TOOLCHAIN_PATH)
include extension-ci-tools/makefiles/duckdb_extension.Makefile