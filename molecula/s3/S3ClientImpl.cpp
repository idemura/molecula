#include "molecula/s3/S3ClientImpl.hpp"

namespace molecula {
std::unique_ptr<S3Client> createS3Client(const S3ClientConfig& config) {
  return std::make_unique<S3ClientImpl>(config);
}

S3ClientImpl::S3ClientImpl(const S3ClientConfig& config) : config_{config} {
  // Empty
}

} // namespace molecula
