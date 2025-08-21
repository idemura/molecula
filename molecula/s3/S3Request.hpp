#pragma once

#include "molecula/http_client/HttpClient.hpp"

#include <ctime>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace molecula {

class S3Time {
public:
    static constexpr size_t kBufferSize = 24;

    S3Time();
    explicit S3Time(std::time_t timestamp);

    std::string_view getDate(char* buffer) const;
    std::string_view getDateTime(char* buffer) const;

private:
    std::time_t timestamp{};
    std::tm tm{};
};

// S3 request.
class S3Request {
public:
    HttpHeaders headers;
    HttpMethod method{HttpMethod::GET};
    ByteSpan body;

    void setHost(std::string host);

    std::string_view getHost() const {
        return host;
    }

    void setPath(std::string path);

    std::string_view getPath() const {
        return path;
    }

    void setQuery(std::string query);

    std::string_view getQuery() const {
        return query;
    }

    void prepareToSign(const S3Time& time);
    void appendHeaderNames(std::string& output) const;

    // @prepareToSign should be called first.
    std::string getRequestTextToHash() const;

    std::string getRequestHash() const;

    std::string_view getBodyHash() const {
        return bodyHash;
    }

private:
    std::string host;
    std::string path;
    std::string query;
    std::string bodyHash;
};

class S3SignerV4 {
public:
    S3SignerV4(std::string_view accessKey, std::string_view secretKey, std::string_view region);

    void generateSigningKey(const S3Time& time);

    std::string_view getSigningKey() const {
        return {signingKey, sizeof(signingKey)};
    }

    std::string getSigningKeyHex() const;

    void sign(S3Request& request, const S3Time& time);

private:
    std::string accessKey;
    std::string secretKey;
    std::string region;

    // SHA256_DIGEST_LENGTH is 32 bytes.
    char signingKey[32]{};
};

} // namespace molecula
