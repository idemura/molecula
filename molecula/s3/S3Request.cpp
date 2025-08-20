#include "molecula/s3/S3Request.hpp"

#include "folly/String.h"

#include <glog/logging.h>

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <cstring>

namespace molecula {

static_assert(SHA256_DIGEST_LENGTH == 32, "SHA256 digest length must be 32 bytes");

// Buffer must be SHA256_DIGEST_LENGTH bytes long.
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

std::string cryptoSha256Hex(ByteSpan data) {
  char buffer[SHA256_DIGEST_LENGTH];
  SHA256((const unsigned char*)data.data(), data.size(), (unsigned char*)buffer);
  return folly::hexlify({buffer, SHA256_DIGEST_LENGTH});
}

S3Time::S3Time() : S3Time{std::time(nullptr)} {}

S3Time::S3Time(std::time_t timestamp) : timestamp_{timestamp} {
  ::gmtime_r(&timestamp_, &tm_);
}

std::string_view S3Time::getDate(char* buffer) const {
  auto length = std::strftime(buffer, kBufferSize, "%Y%m%d", &tm_);
  return {buffer, length};
}

std::string_view S3Time::getDateTime(char* buffer) const {
  auto length = std::strftime(buffer, kBufferSize, "%Y%m%dT%H%M%SZ", &tm_);
  return {buffer, length};
}

S3SignerV4::S3SignerV4(
    std::string_view accessKey,
    std::string_view secretKey,
    std::string_view region) :
    accessKey_{accessKey}, secretKey_{secretKey}, region_{region} {}

void S3SignerV4::generateSigningKey(const S3Time& time) {
  char keys[2][SHA256_DIGEST_LENGTH];

  // Initalize first key with "AWS4" + secret key
  keys[0][0] = 'A';
  keys[0][1] = 'W';
  keys[0][2] = 'S';
  keys[0][3] = '4';
  std::memcpy(&keys[0][4], secretKey_.data(), secretKey_.size());
  std::string_view key{keys[0], 4 + secretKey_.size()};

  char buffer[S3Time::kBufferSize];
  key = cryptoHmacSha256(key, time.getDate(buffer), keys[1]);
  key = cryptoHmacSha256(key, region_, keys[0]);
  key = cryptoHmacSha256(key, std::string_view{"s3"}, keys[1]);
  key = cryptoHmacSha256(key, std::string_view{"aws4_request"}, signingKey_);
}

std::string S3SignerV4::getSigningKeyHex() const {
  return folly::hexlify({signingKey_, sizeof(signingKey_)});
}

void S3SignerV4::sign(S3Request& request, const S3Time& time) {
  char buffer[S3Time::kBufferSize];

  generateSigningKey(time);

  request.prepareToSign(time);

  std::string scope;
  scope.reserve(64);
  scope.append(time.getDate(buffer));
  scope.append("/");
  scope.append(region_);
  scope.append("/s3/aws4_request");

  std::string toSign;
  toSign.reserve(256);
  toSign.append("AWS4-HMAC-SHA256\n");
  toSign.append(time.getDateTime(buffer));
  toSign.append("\n");
  toSign.append(scope);
  toSign.append("\n");
  toSign.append(request.getRequestHash());

  char hash[SHA256_DIGEST_LENGTH];
  cryptoHmacSha256(getSigningKey(), toSign, hash);
  std::string signature = folly::hexlify({hash, sizeof(hash)});

  std::string auth;
  auth.reserve(256);
  auth.append("AWS4-HMAC-SHA256 ");
  auth.append("Credential=");
  auth.append(accessKey_);
  auth.append("/");
  auth.append(scope);
  auth.append(",SignedHeaders=");
  request.appendHeaderNames(auth);
  auth.append(",Signature=");
  auth.append(signature);

  request.headers.add(makeHeader("authorization", auth));
}

void S3Request::setHost(std::string host) {
  host_ = std::move(host);
}

void S3Request::setPath(std::string path) {
  if (path.empty()) {
    path_ = "/";
  } else if (path[0] == '/') {
    path_ = std::move(path);
  } else {
    path_.clear();
    path_.append("/").append(path);
  }
}

void S3Request::setQuery(std::string query) {
  query_ = std::move(query);
}

void S3Request::appendHeaderNames(std::string& output) const {
  bool semicolon = false;
  for (const std::string& header : headers.list()) {
    if (semicolon) {
      output.append(";");
    } else {
      semicolon = true;
    }
    size_t i = header.find(':');
    if (i != std::string::npos) {
      output.append(header.data(), i);
    } else {
      output.append(header);
    }
  }
}

void S3Request::prepareToSign(const S3Time& time) {
  char buffer[S3Time::kBufferSize];

  bodyHash_ = cryptoSha256Hex(body);

  headers.add(makeHeader("host", host_));
  headers.add(makeHeader("x-amz-date", time.getDateTime(buffer)));
  headers.add(makeHeader("x-amz-content-sha256", bodyHash_));

  headers.sort();
}

std::string S3Request::getRequestTextToHash() const {
  std::string output;
  output.reserve(256);

  // All path element must be URL-encoded and canonical.
  output.append(getMethodName(method));
  output.append("\n");
  output.append(path_);
  output.append("\n");
  output.append(query_);
  output.append("\n");

  // Add headers
  for (const std::string& header : headers.list()) {
    output.append(header).append("\n");
  }
  output.append("\n");

  // Add signed headers
  appendHeaderNames(output);
  output.append("\n");

  output.append(getBodyHash());

  return output;
}

std::string S3Request::getRequestHash() const {
  auto text = getRequestTextToHash();
  return cryptoSha256Hex(ByteSpan{text.data(), text.size()});
}

} // namespace molecula
