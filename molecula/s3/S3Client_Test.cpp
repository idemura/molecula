#include "molecula/s3/S3Client.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(S3Client, S3SigV4) {
  S3SigV4 sig{"access_key", "secret_key", "us-east-1"};

  std::vector<std::string> headers{"header1: value1", "header2: value2"};
  auto signature = sig.sign("bucket", "object/name", headers, std::string{""});
  EXPECT_EQ(signature, "expected_signature_value");
}

} // namespace molecula
