#pragma once

#include "folly/futures/Future.h"
#include "molecula/http_client/HttpClient.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace molecula {

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
  S3GetObjectInfoRequest(std::string_view bucket, std::string_view key) :
      bucket{bucket}, key{key} {}

  std::string_view bucket;
  std::string_view key;
};

class S3GetObjectInfo {
public:
  explicit S3GetObjectInfo(HttpResponse response);

  long status{};
  std::string etag;
  long size{};
  std::time_t lastModified{};
};

class S3GetObjectRequest {
public:
  S3GetObjectRequest(std::string_view bucket, std::string_view key) : bucket{bucket}, key{key} {}

  std::string_view bucket;
  std::string_view key;
  long range[2]{};

  void setRange(long begin, long end);

  bool hasRange() const {
    return range[0] > 0 || range[1] > 0;
  }

  std::string getRangeHeader() const;
};

class S3GetObject {
public:
  explicit S3GetObject(HttpResponse response);

  long status{};
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
