#include "molecula/s3/S3ClientImpl.hpp"

#include <glog/logging.h>

namespace molecula {
std::unique_ptr<S3Client> createS3Client(HttpClient* httpClient, const S3ClientConfig& config) {
  return std::make_unique<S3ClientImpl>(httpClient, config);
}

S3ClientImpl::S3ClientImpl(HttpClient* httpClient, const S3ClientConfig& config)
    : httpClient_{httpClient}, endpoint_{config.endpoint}, config_{config} {
  if (!endpoint_.empty() && endpoint_.back() == '/') {
    endpoint_.pop_back(); // Remove trailing slash if present
  }
}

std::string S3ClientImpl::getObjectUrl(std::string_view bucket, std::string_view key) const {
  return std::string{endpoint_}.append("/").append(bucket).append("/").append(key);
}

folly::Future<S3GetObjectRes> S3ClientImpl::getObject(const S3GetObjectReq& req) {
  HttpRequest request{getObjectUrl(req.bucket, req.key)};
  LOG(INFO) << "S3 GET Object request URL: " << request.getUrl();
  request.setMethod(HttpMethod::GET);
  auto response = httpClient_->makeRequest(std::move(request)).get();
  LOG(INFO) << "S3 GET Object response status: " << response.getStatus();
  return folly::Future<S3GetObjectRes>(S3GetObjectRes{});
}

} // namespace molecula
