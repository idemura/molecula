#pragma once

#include "folly/futures/Future.h"
#include "molecula/http_client/HttpClient.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace molecula {

using ByteSpan = std::span<const char>;

class S3ClientConfig {
public:
  std::string_view endpoint;
  std::string_view accessKey;
  std::string_view secretKey;
  std::string_view region;
  bool pathStyle{true};
};

class S3GetObjectInfoRequest {
public:
  std::string_view bucket;
  std::string_view key;

  S3GetObjectInfoRequest(std::string_view bucket, std::string_view key)
      : bucket{bucket}, key{key} {}
};

class S3GetObjectInfo {
public:
  explicit S3GetObjectInfo(HttpResponse response)
      : status{response.getStatus()}, etag{response.getHeaderValue("etag")} {}

  long status{0};
  std::string etag;
};

class S3GetObjectRequest {
public:
  std::string_view bucket;
  std::string_view key;
  long range[2]{};

  S3GetObjectRequest(std::string_view bucket, std::string_view key) : bucket{bucket}, key{key} {}

  void setRange(long begin, long end) {
    range[0] = begin;
    range[1] = end;
  }
};

class S3GetObject {
public:
  explicit S3GetObject(HttpResponse response)
      : status{response.getStatus()}, data{std::move(response.getBody())} {}

  long status{0};
  ByteBuffer data;
};

// S3 storage client.
class S3Client {
public:
  virtual ~S3Client() = default;
  virtual folly::Future<S3GetObjectInfo> getObjectInfo(const S3GetObjectInfoRequest& req) = 0;
  virtual folly::Future<S3GetObject> getObject(const S3GetObjectRequest& req) = 0;
};

std::unique_ptr<S3Client> createS3Client(HttpClient* httpClient, const S3ClientConfig& config);

} // namespace molecula
