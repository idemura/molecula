#pragma once

#include "molecula/common/ByteBuffer.hpp"

#include <memory>
#include <string_view>

namespace molecula {

// Iceberg table metadata.
class IcebergMetadata {
public:
    static std::unique_ptr<IcebergMetadata> fromJson(ByteBuffer& buffer);

    IcebergMetadata() = default;

private:
    //
};

} // namespace molecula
