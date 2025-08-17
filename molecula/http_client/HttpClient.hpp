#pragma once

#include <memory>

namespace molecula {
/// Async HTTP client based on libevent.
class HttpClient {
 public:
  virtual ~HttpClient() = default;
  virtual void makeRequest(const char* host, int port, const char* path) = 0;
};

std::unique_ptr<HttpClient> createHttpClient();
} // namespace molecula
