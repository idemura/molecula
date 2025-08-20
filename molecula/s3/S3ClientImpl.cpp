#include "molecula/s3/S3ClientImpl.hpp"

#include <glog/logging.h>
#include <charconv>

namespace molecula {
std::unique_ptr<S3Client> createS3Client(HttpClient* httpClient, const S3ClientConfig& config) {
  return std::make_unique<S3ClientImpl>(httpClient, config);
}

S3ClientImpl::S3ClientImpl(HttpClient* httpClient, const S3ClientConfig& config)
    : httpClient_{httpClient},
      endpoint_{config.endpoint},
      signer_{config.accessKey, config.secretKey, config.region},
      config_{config} {}

void S3ClientImpl::setObject(S3Request& request, std::string_view bucket, std::string_view key)
    const {
  if (config_.pathStyle) {
    request.setHost(endpoint_.host());
    request.setPath(std::string{"/"}.append(bucket).append("/").append(key));
  } else {
    request.setHost(std::string{bucket}.append(".").append(endpoint_.host()));
    request.setPath(std::string{"/"}.append(key));
  }
}

std::string S3ClientImpl::buildUrl(const S3Request& request) const {
  std::string url;
  url.append(endpoint_.scheme());
  url.append("://");
  url.append(request.getHost());
  if (endpoint_.port() > 0) {
    url.append(":");
    char buf[16];
    auto [ptr, ec] = std::to_chars(std::begin(buf), std::end(buf), endpoint_.port());
    url.append(buf, ptr - buf);
  }
  url.append(request.getPath());
  if (!request.getQuery().empty()) {
    url.append("?");
    url.append(request.getQuery());
  }
  return url;
}

folly::Future<S3GetObjectRes> S3ClientImpl::getObject(const S3GetObjectReq& req) {
  S3Time time;

  S3Request s3Req;
  s3Req.setMethod(HttpMethod::GET);
  setObject(s3Req, req.bucket, req.key);

  std::string auth = signer_.sign(s3Req, time);

  // Build HTTP request URL
  HttpRequest request{buildUrl(s3Req)};
  request.setMethod(HttpMethod::GET);
  for (auto& header : s3Req.getHeaders()) {
    request.addHeader(std::move(header));
  }
  request.addHeader(makeHeader("authorization", auth));

  auto response = httpClient_->makeRequest(std::move(request)).get();
  return folly::Future<S3GetObjectRes>(
      S3GetObjectRes{.status = response.getStatus(), .data = std::move(response.getBody())});
}

} // namespace molecula
