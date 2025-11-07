#ifndef DUCKDP_EXTENSION_HPP
#define DUCKDP_EXTENSION_HPP

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

class DuckdpExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

// Forward declaration for benchmark pragma function
void DPSumBenchmarkPragma(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // DUCKDP_EXTENSION_HPP
