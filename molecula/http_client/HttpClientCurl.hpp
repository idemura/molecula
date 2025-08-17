#pragma once

#include <curl/curl.h>
#include <mutex>
#include <thread>
#include "molecula/http_client/HttpClient.hpp"

namespace molecula {
/// Async HTTP client based on libevent.
class HttpClientCurl final : public HttpClient {
 public:
  HttpClientCurl(CURLM* curlMultiHandle, const HttpClientParams& params)
      : curlMultiHandle_{curlMultiHandle}, params_{params} {}

  ~HttpClientCurl() override;
  folly::Future<HttpResponse> makeRequest(const char* url) override;

 private:
  CURLM* curlMultiHandle_{nullptr};
  HttpClientParams params_;
};
} // namespace molecula
