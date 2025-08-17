#pragma once

#include <curl/curl.h>
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include "molecula/http_client/HttpClient.hpp"

namespace molecula {

class HttpContext {
public:
  HttpContext(HttpRequest request) : request{std::move(request)}, buffer{0} {}

  HttpRequest request;
  folly::Promise<HttpResponse> promise;
  HttpBuffer buffer;
};

/// Async HTTP client based on libevent.
class HttpClientCurl final : public HttpClient {
public:
  HttpClientCurl(CURLM* multiHandle, const HttpClientParams& params);
  ~HttpClientCurl() override;
  folly::Future<HttpResponse> makeRequest(HttpRequest request) override;

private:
  void eventLoop();

  CURLM* multiHandle_{nullptr};
  std::mutex mutex_;
  std::queue<HttpContext*> queue_;
  int pipe_[2]{}; // Pipe for signaling
  std::atomic<bool> running_{true};
  std::thread eventThread_;
  HttpClientParams params_;
};

} // namespace molecula
