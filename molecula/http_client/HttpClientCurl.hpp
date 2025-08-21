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
  static std::unique_ptr<HttpClient> create(const HttpClientConfig& config);

  HttpClientCurl(void* multiHandle, const HttpClientConfig& config);
  ~HttpClientCurl() override;
  folly::Future<HttpResponse> makeRequest(HttpRequest request) override;

private:
  void eventLoop();
  HttpContext* takeNextFromQueue();
  void* createEasyHandle(HttpContext* context);

  static std::mutex globalMutex;
  static long numClients;

  void* multiHandle{};
  std::mutex mutex;
  long counter{1'000};
  std::queue<HttpContext*> queue;
  int pipe[2]{}; // Pipe for signaling
  std::atomic<bool> running{true};
  std::thread eventThread;
  HttpClientConfig config;
};

} // namespace molecula
