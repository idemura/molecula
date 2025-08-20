#include "molecula/s3/S3Client.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(S3Client, S3SigV4) {
  S3SigV4 sig{"access_key", "secret_key", "us-east-1"};

  sig.generateSigningKey();
  EXPECT_EQ(
      sig.getSigningKey(), "54f20e06ff19d00e2411978da9fba775dfddfe5bc105cdc3b472e12ceb4f579b");

  std::vector<std::string> headers{"Header1: value1", "Header2: value2"};
  auto signature = sig.sign("bucket", "object/name", headers, std::string{""});
  EXPECT_EQ(signature, "");
}

} // namespace molecula
