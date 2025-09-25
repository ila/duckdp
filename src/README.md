# DuckDB Differential Privacy (DP) Extension

This extension integrates Google Differential Privacy (DP) algorithms (such as BoundedSum) into DuckDB.

## Important extra prerequisites (in addition to the steps below)

Before building this extension, you must also:

1) Compile and install Abseil (static, with PIC)
   - Example:
     - git clone https://github.com/abseil/abseil-cpp.git
     - cd abseil-cpp && mkdir build && cd build
     - cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DABSL_BUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX="$PWD/../../extension/dp/third_party/install" ..
     - cmake --build . --target install -j$(nproc)
   - Result: headers under extension/dp/third_party/install/include and static libs under extension/dp/third_party/install/lib

2) Patch Google DP CMakeLists.txt to install the DP library and headers
   - In the Google DP source (differential-privacy), add install rules for the static lib and headers. Example snippet to append in its top-level or cc/CMakeLists.txt (where the dp_algorithms target is defined):
     - install(TARGETS dp_algorithms ARCHIVE DESTINATION lib)
     - install(DIRECTORY cc/ DESTINATION include/dp FILES_MATCHING PATTERN "*.h")
     - Ensure generated protobuf headers (cc/proto/*.pb.h and subdirs like cc/proto/accounting/*.pb.h) are present and installed as well, e.g.:
       - install(DIRECTORY cc/proto/ DESTINATION include/proto FILES_MATCHING PATTERN "*.pb.h")

3) Compile and install the Google DP library
   - Generate protobufs first (see section below), then:
     - mkdir build && cd build
     - cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX=/home/ila/local/dp -Dabsl_DIR="<unused if linking by path>" ..
     - cmake --build . --target install -j$(nproc)
   - Result: libdp_algorithms.a under /home/ila/local/dp/lib and headers under /home/ila/local/dp/include (e.g., /home/ila/local/dp/include/dp/... and /home/ila/local/dp/include/proto/...)

Finally, point this extension to those installs (already wired in CMakeLists.txt):
- ABSL_ROOT defaults to extension/dp/third_party/install
- GOOGLE_DP_INSTALL defaults to /home/ila/local/dp

## Setup Instructions

### 1. Pull Submodules

This extension relies on third-party code (Google DP, Abseil, etc.) included as submodules. Make sure to initialize and update all submodules:

```sh
git submodule update --init --recursive
```

### 2. Install Protobuf Compiler

Google DP requires Protocol Buffers (protobuf) for serialization. Install the protobuf compiler (`protoc`) and development libraries:

**On Ubuntu/Debian:**
```sh
sudo apt-get install protobuf-compiler libprotobuf-dev
sudo apt install libgmock-dev libgtest-dev
sudo apt install libkissfft-dev
```

**On macOS (with Homebrew):**
```sh
brew install protobuf
```

### 3. Generate Protobuf Files

Some Google DP source files require generated protobuf headers and sources. You must generate these from the `.proto` files:

**Important:**
- The Google DP library expects the proto files to be in `extension/dp/third_party/differential-privacy/cc/proto/` and its subdirectories (e.g., `accounting/`).
- If you only see proto files in `extension/dp/third_party/differential-privacy/proto/`, you must copy or symlink them into the `cc/proto/` directory structure.
- If the `cc/proto/accounting/` directory does not exist, create it and copy the `privacy-loss-distribution.proto` file there.

**Example:**
```sh
mkdir -p extension/dp/third_party/differential-privacy/cc/proto/accounting
cp extension/dp/third_party/differential-privacy/proto/accounting/privacy-loss-distribution.proto extension/dp/third_party/differential-privacy/cc/proto/accounting/
```

Now generate the protobuf files:

```sh
cd extension/dp/third_party/differential-privacy
protoc -I=. --cpp_out=cc \
    proto/confidence-interval.proto \
    proto/numerical-mechanism.proto \
    proto/summary.proto \
    proto/data.proto \
    proto/accounting/privacy-loss-distribution.proto
```

This will create `.pb.h` and `.pb.cc` files for each proto file. If you add more DP features, you may need to generate additional proto files.

### 4. Build the Extension

From your DuckDB build directory (e.g., `cmake-build-debug`):

```sh
cmake --build . --target dp_extension -j$(nproc)
```

### 5. Troubleshooting

- Missing `.pb.h` or `.pb.cc` files:
  - Make sure you have generated the protobuf files as described above.
  - Ensure the proto files are in the correct `cc/proto/` directory structure.
  - If you see errors like `No such file or directory: proto/accounting/privacy-loss-distribution.pb.h`, check that the `accounting/` subdirectory exists and contains the generated files.
- Missing proto files:
  - If you are missing proto files or directories, update your submodules:
    ```sh
    git submodule update --init --recursive
    ```
  - If still missing, manually copy the proto files from the Google DP repository into the correct location.
- C++11 variadic macro warning:
  - You may see a warning like `ISO C++11 requires at least one argument for the "..." in a variadic macro`. This is harmless, but you can suppress it by adding `-Wno-variadic-macros` to your compile options.
- Regenerating proto files after updates:
  - If you update the Google DP library or move proto files, always rerun the `protoc` command to regenerate the `.pb.h` and `.pb.cc` files.

### 6. Verifying the Build

After building, you can test the extension by loading it in DuckDB and running a simple query. For example:

```sql
INSTALL dp;
LOAD dp;
-- Example usage (if implemented):
-- SELECT dp_sum(column, epsilon, lower, upper) FROM my_table;
```

---

For more information, see the DuckDB and Google Differential Privacy documentation.
