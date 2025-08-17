#pragma once

#include "molecula/s3/S3Client.hpp"

namespace molecula {

class S3ClientImpl final : public S3Client {
public:
  explicit S3ClientImpl(const S3ClientConfig& config);
  ~S3ClientImpl() override = default;

private:
  S3ClientConfig config_;
};

} // namespace molecula
