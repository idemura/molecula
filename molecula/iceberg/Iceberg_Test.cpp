#include "molecula/iceberg/Iceberg.hpp"

#include "molecula/common/ByteBuffer.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(Iceberg, IcebergMetadataFromJson) {
    ByteBuffer buffer{64};
    buffer.append(R"({"table-uuid": "12345"})");
    auto metadata = IcebergMetadata::fromJson(buffer);
    EXPECT_FALSE(metadata == nullptr);
}

} // namespace molecula
