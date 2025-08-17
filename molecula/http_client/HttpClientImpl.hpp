#pragma once

#include <curl/curl.h>
#include <mutex>
#include <thread>
#include "molecula/http_client/HttpClient.hpp"

namespace molecula {
/// Async HTTP client based on libevent.
class HttpClientImpl final : public HttpClient {
 public:
  HttpClientImpl(CURLM* curlMultiHandle, const HttpClientParams& params)
      : curlMultiHandle_{curlMultiHandle}, params_{params} {}

  ~HttpClientImpl() override;

  std::unique_ptr<HttpRequest> makeRequest(const char* url) override;

 private:
  CURLM* curlMultiHandle_{nullptr};
  HttpClientParams params_;
};
} // namespace molecula
