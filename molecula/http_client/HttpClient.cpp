#include "molecula/http_client/HttpClient.hpp"

#include <cctype>
#include <charconv>

namespace molecula {

static std::string_view kHeaderContentLength{"content-length:"};

std::string& lowerCaseHeader(std::string& header) {
  for (char& c : header) {
    if (c == ':') {
      break; // Stop at the first colon
    }
    c = std::tolower(c);
  }
  return header;
}

std::string_view toStringView(HttpMethod method) {
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

void HttpRequest::addHeader(std::string header) {
  lowerCaseHeader(header);
  headers_.push_back(std::move(header));
}

void HttpResponse::addHeader(std::string header) {
  lowerCaseHeader(header);
  // Check if this is a Content-Length header and allocate buffer
  if (header.starts_with(kHeaderContentLength)) {
    size_t i = kHeaderContentLength.size();
    while (i < header.size() && std::isspace(header[i])) {
      ++i;
    }
    size_t v = 0;
    auto [last, ec] = std::from_chars(header.data() + i, header.data() + header.size(), v);
    if (ec == std::errc() && v > 0) {
      body_.reserve(v);
    }
  }
  headers_.push_back(std::move(header));
}

std::string makeHeader(const char* namez, std::string_view value) {
  std::string header;
  std::string_view name{namez};
  header.reserve(name.size() + value.size() + 1);
  header.append(name);
  header.push_back(':');
  header.append(value);
  return header;
}

} // namespace molecula
