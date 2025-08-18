#include "molecula/s3/S3Client.hpp"

#include <openssl/hmac.h>
#include <openssl/sha.h>

namespace molecula {

S3SigV4::S3SigV4(std::string_view accessKey, std::string_view secretKey, std::string_view region)
    : accessKey_{accessKey}, secretKey_{secretKey}, region_{region} {}

std::string S3SigV4::sign(
    std::string_view bucket,
    std::string_view key,
    std::span<std::string> headers,
    std::span<const char> body) {
  return "";
}

} // namespace molecula
