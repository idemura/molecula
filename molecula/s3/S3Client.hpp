#pragma once

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "folly/futures/Future.h"
#include "molecula/http_client/HttpClient.hpp"

namespace molecula {

using ByteSpan = std::span<const char>;

class S3ClientConfig {
public:
  std::string_view endpoint;
  std::string_view accessKey;
  std::string_view secretKey;
};

class S3GetObjectReq {
public:
  std::string_view bucket;
  std::string_view key;
  long range[2]{};

  S3GetObjectReq(std::string_view bucket, std::string_view key) : bucket{bucket}, key{key} {}

  S3GetObjectReq& setRange(long begin, long end) {
    range[0] = begin;
    range[1] = end;
    return *this;
  }
};

class S3GetObjectRes {
public:
  long status = 0;
  HttpBuffer data;
};

// S3 storage client.
class S3Client {
public:
  virtual ~S3Client() = default;
  virtual folly::Future<S3GetObjectRes> getObject(const S3GetObjectReq& req) = 0;
};

std::unique_ptr<S3Client> createS3Client(HttpClient* httpClient, const S3ClientConfig& config);

} // namespace molecula
