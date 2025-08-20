#include "molecula/s3/S3Client.hpp"

#include <glog/logging.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <cstring>
#include <ctime>

#include "folly/String.h"

namespace molecula {

static std::string_view
cryptoHmacSha256(std::string_view key, std::string_view data, char* buffer) {
  unsigned int hashSize = 0;
  HMAC(
      EVP_sha256(),
      key.data(),
      key.size(),
      (const unsigned char*)data.data(),
      data.size(),
      (unsigned char*)buffer,
      &hashSize);
  return std::string_view{buffer, hashSize};
}

S3SigV4::S3SigV4(std::string accessKey, std::string secretKey, std::string region)
    : accessKey_{std::move(accessKey)},
      secretKey_{std::move(secretKey)},
      region_{std::move(region)} {}

void S3SigV4::generateSigningKey() {
  char keys[2][EVP_MAX_MD_SIZE];

  // Initalize first key with "AWS4" + secret key
  keys[0][0] = 'A';
  keys[0][1] = 'W';
  keys[0][2] = 'S';
  keys[0][3] = '4';
  std::memcpy(&keys[0][4], secretKey_.data(), secretKey_.size());
  std::string_view key{keys[0], 4 + secretKey_.size()};

  // Get current date in YYYYMMDD format
  auto timestamp = std::time(nullptr);
  std::tm gm{/* zero init */};
  gmtime_r(&timestamp, &gm);
  char date[16];
  auto dateLength = std::strftime(date, sizeof(date), "%Y%m%d", &gm);

  key = cryptoHmacSha256(key, std::string_view{date, dateLength}, keys[1]);
  key = cryptoHmacSha256(key, region_, keys[0]);
  key = cryptoHmacSha256(key, std::string_view{"s3"}, keys[1]);
  key = cryptoHmacSha256(key, std::string_view{"aws4_request"}, keys[0]);

  signingKey_.assign(key.data(), key.size());
}

std::string S3SigV4::getSigningKey() const {
  return folly::hexlify({signingKey_.data(), signingKey_.size()});
}

std::string S3SigV4::sign(
    std::string_view bucket,
    std::string_view key,
    std::span<std::string> headers,
    std::span<const char> body) {
  return {};
}

} // namespace molecula
