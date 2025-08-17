#pragma once

#include <string_view>

#include "folly/futures/Future.h"
#include "molecula/http_client/HttpClient.hpp"

namespace molecula {

class S3ClientConfig {
public:
  HttpClient* httpClient{nullptr};
  std::string_view endpoint;
  std::string_view accessKey;
  std::string_view secretKey;
};

// S3 storage client.
class S3Client {
public:
  virtual ~S3Client() = default;
};

std::unique_ptr<S3Client> createS3Client(const S3ClientConfig& config);

} // namespace molecula
