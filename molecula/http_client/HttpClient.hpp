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
  HttpBuffer() = default;

  void reserve(size_t size) {
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

  void addHeader(std::string header);

  std::span<std::string> getHeaders() {
    return headers_;
  }

  HttpBuffer& getBody() {
    return body_;
  }

private:
  std::string url_;
  HttpMethod method_{HttpMethod::GET};
  std::vector<std::string> headers_;
  HttpBuffer body_;
};

class HttpResponse {
public:
  HttpResponse() = default;

  void setStatus(long status) {
    status_ = status;
  }

  long getStatus() {
    return status_;
  }

  void addHeader(std::string header);

  std::span<std::string> getHeaders() {
    return headers_;
  }

  HttpBuffer& getBody() {
    return body_;
  }

private:
  long status_ = 0;
  std::vector<std::string> headers_;
  HttpBuffer body_;
};

/// Async HTTP client.
class HttpClient {
public:
  virtual ~HttpClient() = default;
  virtual folly::Future<HttpResponse> makeRequest(HttpRequest request) = 0;
};

/// HTTP client parameters.
class HttpClientConfig {
public:
  bool reserveContentLength{true};
};

std::unique_ptr<HttpClient> createHttpClientCurl(const HttpClientConfig& config);
std::string& lowerCaseHeader(std::string& header);

} // namespace molecula
