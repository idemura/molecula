#pragma once

#include <memory>
#include <string>
#include <string_view>

namespace molecula {

// Iceberg table metadata.
class IcebergMetadata {
public:
    static std::unique_ptr<IcebergMetadata> fromJson(std::string_view json);

    IcebergMetadata() = default;

private:
    //
};

} // namespace molecula
