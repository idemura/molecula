#pragma once

#include <memory>
#include <string>

#include "molecula/s3/S3Client.hpp"

namespace molecula {

class S3ClientImpl final : public S3Client {
public:
  explicit S3ClientImpl(HttpClient* httpClient, const S3ClientConfig& config);
  ~S3ClientImpl() override = default;
  folly::Future<S3GetObjectRes> getObject(const S3GetObjectReq& req) override;

private:
  std::string getObjectUrl(std::string_view bucket, std::string_view key) const;

  HttpClient* httpClient_{nullptr};
  std::string endpoint_;
  S3ClientConfig config_;
};

} // namespace molecula
