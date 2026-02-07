#include "molecula/http_client/HttpClient.hpp"

#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <charconv>

namespace molecula {

static const std::string_view kHeaderContentLength{"content-length"};

void lowerCaseHeader(std::string &header) {
    for (char &c : header) {
        if (c == ':') {
            break; // Stop at the first colon
        }
        c = std::tolower(c);
    }
}

std::string makeHeader(std::string_view name, std::string_view value) {
    std::string header;
    header.reserve(name.size() + value.size() + 1);
    header.append(name);
    header.push_back(':');
    header.append(value);
    return header;
}

std::string_view getMethodName(HttpMethod method) {
    switch (method) {
    case HttpMethod::GET:
        return "GET";
    case HttpMethod::HEAD:
        return "HEAD";
    case HttpMethod::POST:
        return "POST";
    case HttpMethod::PUT:
        return "PUT";
    case HttpMethod::DELETE:
        return "DELETE";
    }
    return "";
}

void HttpHeaders::add(std::string header) {
    lowerCaseHeader(header);
    headers.push_back(std::move(header));
}

void HttpHeaders::sort() {
    std::sort(headers.begin(), headers.end());
}

std::string_view HttpHeaders::get(std::string_view name) const {
    for (const std::string &header : headers) {
        if (header.starts_with(name)) {
            size_t i = name.size();
            if (i == header.size()) {
                return {};
            }
            if (header[i] == ':') {
                ++i; // Skip ':'
                while (i < header.size() && std::isspace(header[i])) {
                    ++i;
                }
                return std::string_view{header}.substr(i);
            }
        }
    }
    return {};
}

long HttpHeaders::getLong(std::string_view name) const {
    std::string_view value = get(name);
    if (value.empty()) {
        return 0;
    } else {
        long result = 0;
        std::from_chars(value.data(), value.data() + value.size(), result);
        return result;
    }
}

size_t HttpHeaders::getContentLength() const {
    return getLong(kHeaderContentLength);
}

void HttpResponse::appendToBody(const char *data, size_t size) {
    if (body.capacity() == 0) {
        constexpr size_t kMaxReserve = 16 * 1024 * 1024; // 16MB
        body.reserve(std::min(headers.getContentLength(), kMaxReserve));
    }
    body.append(data, size);
}

} // namespace molecula
