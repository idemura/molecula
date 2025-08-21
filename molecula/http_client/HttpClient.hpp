#pragma once

#include "folly/Uri.h"
#include "folly/futures/Future.h"

#include "molecula/common/ByteBuffer.hpp"

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace molecula {

enum class HttpMethod {
    GET,
    HEAD,
    POST,
    PUT,
    DELETE,
};

class HttpHeaders {
public:
    void add(std::string header);
    void sort();

    std::span<std::string> span() {
        return headers;
    }

    std::span<const std::string> span() const {
        return headers;
    }

    std::string_view get(std::string_view name) const;
    long getLong(std::string_view name) const;
    size_t getContentLength() const;

private:
    std::vector<std::string> headers;
};

class HttpRequest {
public:
    std::string url;
    HttpMethod method{HttpMethod::GET};
    HttpHeaders headers;
    ByteBuffer body;
};

class HttpResponse {
public:
    long status{};
    HttpHeaders headers;
    ByteBuffer body;

    void appendToBody(const char* data, size_t size);
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
std::string makeHeader(std::string_view name, std::string_view value);
void lowerCaseHeader(std::string& header);
std::string_view getMethodName(HttpMethod method);

inline bool is1xx(long status) {
    return status >= 100 && status < 200;
}

inline bool is2xx(long status) {
    return status >= 200 && status < 300;
}

inline bool is3xx(long status) {
    return status >= 300 && status < 400;
}

inline bool is4xx(long status) {
    return status >= 400 && status < 500;
}

inline bool is5xx(long status) {
    return status >= 500 && status < 600;
}

} // namespace molecula
