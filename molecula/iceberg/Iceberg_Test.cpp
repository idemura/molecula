#include "molecula/iceberg/Iceberg.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(Iceberg, IcebergMetadataFromJson) {
    auto metadata = IcebergMetadata::fromJson("{}");
    EXPECT_TRUE(true);
}

} // namespace molecula
