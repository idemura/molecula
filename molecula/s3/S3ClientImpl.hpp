#pragma once

#include "molecula/s3/S3Client.hpp"
#include "molecula/s3/S3Request.hpp"

#include <memory>
#include <string>

namespace molecula {

class S3ClientImpl final : public S3Client {
public:
  explicit S3ClientImpl(HttpClient* httpClient, const S3ClientConfig& config);
  ~S3ClientImpl() override = default;
  folly::Future<S3GetObjectInfo> getObjectInfo(const S3GetObjectInfoRequest& req) override;
  folly::Future<S3GetObject> getObject(const S3GetObjectRequest& req) override;

private:
  void setObject(S3Request& request, std::string_view bucket, std::string_view key) const;
  HttpRequest createHttpRequest(S3Request& request) const;

  HttpClient* httpClient_{};
  folly::Uri endpoint_;
  S3SignerV4 signer_;
  S3ClientConfig config_;
};

} // namespace molecula
