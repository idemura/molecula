#pragma once

#include <memory>
#include <string>

namespace molecula {
class HttpRequest {
 public:
  void setHttpStatus(long status) {
    httpStatus_ = status;
  }

  long getHttpStatus() {
    return httpStatus_;
  }

  void appendData(const char* data, size_t size) {
    body_.append(data, size);
  }

  std::string_view getBody() {
    return body_;
  }

 private:
  long httpStatus_{0};
  std::string body_;
};

/// Async HTTP client based on libevent.
class HttpClient {
 public:
  virtual ~HttpClient() = default;
  virtual std::unique_ptr<HttpRequest> makeRequest(const char* url) = 0;
};

class HttpClientParams {
 public:
};

std::unique_ptr<HttpClient> createHttpClient(const HttpClientParams& params);
} // namespace molecula
