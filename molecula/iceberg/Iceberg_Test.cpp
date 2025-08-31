#include "molecula/iceberg/Iceberg.hpp"

#include "molecula/common/ByteBuffer.hpp"

#include <gtest/gtest.h>

namespace molecula::iceberg {

GTEST_TEST(Iceberg, MetadataFromJson) {
    ByteBuffer buffer{64};
    buffer.append(R"({"table-uuid": "12345"})");
    // auto metadata = Metadata::fromJson(buffer);
    // EXPECT_EQ(metadata->tableUuid, "12345");
}

} // namespace molecula::iceberg
