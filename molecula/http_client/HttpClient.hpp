#pragma once

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>
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
    if (data_.size() == 0) {
      return nullptr;
    } else {
      return data_.data();
    }
  }

private:
  std::string data_;
};

enum class HttpMethod {
  GET,
  HEAD,
  POST,
  PUT,
  DELETE,
};

class HttpRequest {
public:
  explicit HttpRequest(const char* url) : url_{url} {}
  explicit HttpRequest(std::string url) : url_{std::move(url)} {}

  // Guaranteed to return a 0-terminated string.
  std::string_view getUrl() const {
    return url_;
  }

  void setMethod(HttpMethod method) {
    method_ = method;
  }

  HttpMethod getMethod() const {
    return method_;
  }

  void addHeader(std::string header) {
    headers_.push_back(std::move(header));
  }

  std::span<std::string> getHeaders() {
    return headers_;
  }

private:
  std::string url_;
  HttpMethod method_{HttpMethod::GET};
  std::vector<std::string> headers_;
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
