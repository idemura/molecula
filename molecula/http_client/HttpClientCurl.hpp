#pragma once

#include "molecula/http_client/HttpClient.hpp"

#include <atomic>
#include <mutex>
#include <queue>
#include <thread>

namespace molecula {

class HttpContext;

/// Async HTTP client based on CURL.
class HttpClientCurl final : public HttpClient {
public:
  HttpClientCurl(void* multiHandle, const HttpClientConfig& config);
  ~HttpClientCurl() override;
  folly::Future<HttpResponse> makeRequest(HttpRequest request) override;

private:
  void eventLoop();
  HttpContext* takeNextFromQueue();
  void* createEasyHandle(HttpContext* context);

  void* multiHandle_{nullptr};
  std::mutex mutex_;
  long counter_{1'000};
  std::queue<HttpContext*> queue_;
  int pipe_[2]{}; // Pipe for signaling
  std::atomic<bool> running_{true};
  std::thread eventThread_;
  HttpClientConfig config_;
};

} // namespace molecula
