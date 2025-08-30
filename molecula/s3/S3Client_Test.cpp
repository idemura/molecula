#include "molecula/s3/S3Client.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(S3, S3Id_Schemas) {
    EXPECT_EQ(S3Id::fromString("s3://bucket/key").schema(), "s3");
    EXPECT_EQ(S3Id::fromString("s3a://bucket/key").schema(), "s3a");
    EXPECT_EQ(S3Id::fromString("s3b://bucket/key").schema(), "s3b");
}

GTEST_TEST(S3, S3Id_Buckets) {
    EXPECT_EQ(S3Id::fromString("s3://bucket/key").bucket(), "bucket");
    EXPECT_EQ(S3Id::fromString("s3://bucket/path/key").bucket(), "bucket");
    EXPECT_EQ(S3Id::fromString("s3a://bucket/key").bucket(), "bucket");
    EXPECT_EQ(S3Id::fromString("s3a://bucket/path/key").bucket(), "bucket");
}

GTEST_TEST(S3, S3Id_EmptyKey) {
    auto id = S3Id::fromString("s3://bucket/");
    EXPECT_FALSE(id.empty());
    EXPECT_EQ(id.bucket(), "bucket");
    EXPECT_EQ(id.key(), "");
}

GTEST_TEST(S3, S3Id_Paths) {
    EXPECT_EQ(S3Id::fromString("s3://bucket/path/key").key(), "path/key");
    EXPECT_EQ(S3Id::fromString("s3://bucket//path/key").key(), "/path/key");
    EXPECT_EQ(S3Id::fromString("s3://bucket/path/key/").key(), "path/key/");
}

GTEST_TEST(S3, S3Id_Errors) {
    EXPECT_TRUE(S3Id::fromString("invalid_uri").empty());
    EXPECT_TRUE(S3Id::fromString("s3//bucket/key").empty());
    EXPECT_TRUE(S3Id::fromString("s3:/bucket/key").empty());
    EXPECT_TRUE(S3Id::fromString("s3://bucket").empty());
    EXPECT_TRUE(S3Id::fromString("s3:///").empty());
    EXPECT_TRUE(S3Id::fromString("s3://").empty());
}

} // namespace molecula
