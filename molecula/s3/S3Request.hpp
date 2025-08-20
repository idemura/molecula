#pragma once

#include "molecula/http_client/HttpClient.hpp"

#include <ctime>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace molecula {

using ByteSpan = std::span<const char>;

class S3Time {
public:
  static constexpr size_t kBufferSize = 24;

  S3Time();
  explicit S3Time(std::time_t timestamp);

  std::string_view getDate(char* buffer) const;
  std::string_view getDateTime(char* buffer) const;

private:
  std::time_t timestamp_{};
  std::tm gm_{/* zero init */};
};

// S3 request.
class S3Request {
public:
  void setMethod(HttpMethod method) {
    method_ = method;
  }

  HttpMethod getMethod(HttpMethod method) const {
    return method_;
  }

  void setHost(std::string host) {
    host_ = std::move(host);
  }

  std::string_view getHost() const {
    return host_;
  }

  void setPath(std::string path);

  std::string_view getPath() const {
    return path_;
  }

  void setQuery(std::string query) {
    query_ = std::move(query);
  }

  std::string_view getQuery() const {
    return query_;
  }

  void setBody(ByteSpan body) {
    body_ = body;
  }

  ByteSpan getBody() const {
    return body_;
  }

  void addHeader(std::string header);

  std::span<std::string> getHeaders() {
    return headers_;
  }

  void prepareToSign(const S3Time& time);
  void appendHeaderNames(std::string& output) const;

  // First, canonizalize should be called.
  std::string getRequestTextToHash() const;

  std::string getRequestHash() const;

  std::string_view getBodyHash() const {
    return bodyHash_;
  }

private:
  HttpMethod method_{HttpMethod::GET};
  std::string host_;
  std::string path_;
  std::string query_;
  std::vector<std::string> headers_;
  ByteSpan body_;
  std::string bodyHash_;
};

class S3SignerV4 {
public:
  S3SignerV4(std::string_view accessKey, std::string_view secretKey, std::string_view region);

  void generateSigningKey(const S3Time& time);

  std::string_view getSigningKey() const {
    return {signingKey_, sizeof(signingKey_)};
  }

  std::string getSigningKeyHex() const;

  std::string sign(S3Request& request, const S3Time& time);

private:
  std::string accessKey_;
  std::string secretKey_;
  std::string region_;

  // SHA256_DIGEST_LENGTH is 32 bytes.
  char signingKey_[32]{};
};

} // namespace molecula
