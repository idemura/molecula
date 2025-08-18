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

void HttpRequest::addHeader(std::string header) {
  lowerCaseHeader(header);
  headers_.push_back(std::move(header));
}

void HttpResponse::addHeader(std::string header) {
  lowerCaseHeader(header);
  // Check if this is a Content-Length header
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

} // namespace molecula
