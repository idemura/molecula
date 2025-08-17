#pragma once

#include <memory>
#include <string>
#include "folly/futures/Future.h"

namespace molecula {

class HttpBuffer {
public:
  explicit HttpBuffer(size_t size) {
    data_.reserve(size);
  }

  void append(const char* data, size_t size) {
    data_.append(data, size);
  }

  size_t size() const {
    return data_.size();
  }

  const char* data() {
    return data_.data();
  }

private:
  std::string data_;
};

class HttpRequest {
public:
  explicit HttpRequest(const char* url) : url_{url} {}
  explicit HttpRequest(std::string url) : url_{std::move(url)} {}

  const std::string& getUrl() const {
    return url_;
  }

private:
  std::string url_;
};

class HttpResponse {
public:
  HttpResponse(long status, HttpBuffer body) : status_{status}, body_{std::move(body)} {}

  void setStatus(long status) {
    status_ = status;
  }

  long getStatus() {
    return status_;
  }

  HttpBuffer& getBody() {
    return body_;
  }

private:
  long status_ = 0;
  HttpBuffer body_;
};

/// Async HTTP client based on libcurl.
class HttpClient {
public:
  virtual ~HttpClient() = default;
  virtual folly::Future<HttpResponse> makeRequest(HttpRequest request) = 0;
};

/// HTTP client parameters.
class HttpClientParams {
public:
};

std::unique_ptr<HttpClient> createHttpClient(const HttpClientParams& params);

} // namespace molecula
