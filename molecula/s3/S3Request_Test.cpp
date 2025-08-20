#include "molecula/s3/S3Request.hpp"

#include <gtest/gtest.h>

namespace molecula {

const std::string_view kAccessKey{"AKIAIOSFODNN7EXAMPLE"};
const std::string_view kSecretKey{"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"};

GTEST_TEST(S3, S3Time) {
  char buffer[S3Time::kBufferSize];

  S3Time t1{1'755'675'060L};
  EXPECT_EQ(t1.getDate(buffer), "20250820");
  EXPECT_EQ(t1.getDateTime(buffer), "20250820T073100Z");

  S3Time t2{1'755'775'060L};
  EXPECT_EQ(t2.getDate(buffer), "20250821");
  EXPECT_EQ(t2.getDateTime(buffer), "20250821T111740Z");
}

GTEST_TEST(S3, S3RequestPrepareToSign) {
  S3Request request;
  request.setMethod(HttpMethod::GET);
  request.setHost("localhost");
  request.setPath("/bucket/object/name");
  request.addHeader("No-Colon");
  request.addHeader("My-Header:V2");
  request.addHeader("name:V1");
  request.prepareToSign(S3Time{1'755'675'060L});

  std::string headers;
  request.appendHeaderNames(headers);
  EXPECT_EQ(headers, "host;my-header;name;no-colon;x-amz-content-sha256;x-amz-date");

  // Empty body hash
  EXPECT_EQ(
      request.getBodyHash(), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

GTEST_TEST(S3, S3SigV4_SigningKey) {
  S3SigV4 sig{kAccessKey, kSecretKey, "us-east-1"};

  sig.generateSigningKey(S3Time{1'755'675'060L});
  EXPECT_EQ(
      sig.getSigningKeyHex(), "aa4bd5b1bafd943c834c310275c5edd665161c708156d184de060fa8f6a1a242");
  sig.generateSigningKey(S3Time{1'755'685'060L}); // Same day
  EXPECT_EQ(
      sig.getSigningKeyHex(), "aa4bd5b1bafd943c834c310275c5edd665161c708156d184de060fa8f6a1a242");
  sig.generateSigningKey(S3Time{1'755'775'060L}); // Next day
  EXPECT_EQ(
      sig.getSigningKeyHex(), "b54a4bbaf6a28023a1832e875b095fd4f8d4244f93b09a5029d4f1cac1b85c2b");
}

// Amazon example
GTEST_TEST(S3Request, S3SigV4_SigningKey) {
  S3SigV4 sig{kAccessKey, kSecretKey, "us-east-1"};

  S3Request request;
  request.setMethod(HttpMethod::GET);
  request.setHost("examplebucket.s3.amazonaws.com");
  request.setPath("/test.txt");
  request.addHeader("range:bytes=0-9");
  EXPECT_EQ(
      sig.sign(request, S3Time{1'369'353'600L}),
      "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");
}

} // namespace molecula
