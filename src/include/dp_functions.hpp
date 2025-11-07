#pragma once

#include <string>

namespace duckdb {

class ExtensionLoader;

// Grouped registration entry points
void RegisterDPSum(ExtensionLoader &loader);
void RegisterDPLaplace(ExtensionLoader &loader);
void RegisterDPGaussian(ExtensionLoader &loader);

} // namespace duckdb

