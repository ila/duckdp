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

} // namespace duckdb

#endif // DUCKDP_EXTENSION_HPP
