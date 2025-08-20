#include "molecula/s3/S3Client.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(S3Client, S3SigV4) {
  S3SigV4 sig{"access_key", "secret_key", "us-east-1"};

  sig.generateSigningKey(1'755'675'060L);
  EXPECT_EQ(
      sig.getSigningKey(), "54f20e06ff19d00e2411978da9fba775dfddfe5bc105cdc3b472e12ceb4f579b");
  sig.generateSigningKey(1'755'685'060L); // Same day
  EXPECT_EQ(
      sig.getSigningKey(), "54f20e06ff19d00e2411978da9fba775dfddfe5bc105cdc3b472e12ceb4f579b");
  sig.generateSigningKey(1'755'775'060L); // Next day
  EXPECT_EQ(
      sig.getSigningKey(), "644b5e66b7317ad9209dff8e8ed6c8b906ae2258ce683ca68a4f8ee7ffe578c2");

  std::vector<std::string> headers{"Header1: value1", "Header2: value2"};
  auto signature = sig.sign("bucket", "object/name", headers, std::string{""});
  EXPECT_EQ(signature, "");
}

} // namespace molecula
