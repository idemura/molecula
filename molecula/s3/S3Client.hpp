#pragma once

#include "folly/futures/Future.h"
#include "molecula/http_client/HttpClient.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace molecula {

class S3ClientConfig {
public:
    std::string_view endpoint;
    std::string_view accessKey;
    std::string_view secretKey;
    std::string_view region;
    bool pathStyle{true};
};

// S3 object identifier. Stores as a full string for less allocations and better cache locality.
// Parses bucket/key at the construction time.
class S3Id {
public:
    // Returns empty if falied to parse.
    static S3Id fromString(std::string uri);
    static S3Id fromStringView(std::string_view uri);
    static S3Id fromPieces(std::string_view bucket, std::string_view key);

    bool empty() const {
        return str.empty();
    }
    std::string_view uri() const {
        return str;
    }
    std::string_view schema() const {
        return substring(0, bucketStart - 3);
    }
    std::string_view bucket() const {
        return substring(bucketStart, bucketEnd);
    }
    std::string_view key() const {
        return substring(bucketEnd + 1, str.size());
    }

private:
    S3Id() = default;
    S3Id(std::string uri, size_t bucketStart, size_t bucketEnd) :
        str{std::move(uri)}, bucketStart{bucketStart}, bucketEnd{bucketEnd} {}

    std::string_view substring(size_t first, size_t last) const {
        return std::string_view{str.data() + first, last - first};
    }

    std::string str;
    size_t bucketStart{};
    size_t bucketEnd{};
};

class S3GetObjectInfoRequest {
public:
    S3GetObjectInfoRequest(std::string_view bucket, std::string_view key) :
        bucket{bucket}, key{key} {}

    std::string_view bucket;
    std::string_view key;
};

class S3GetObjectInfo {
public:
    explicit S3GetObjectInfo(HttpResponse response);

    long status{};
    std::string etag;
    long size{};
    std::time_t lastModified{};
};

class S3GetObjectRequest {
public:
    S3GetObjectRequest(std::string_view bucket, std::string_view key) : bucket{bucket}, key{key} {}

    std::string_view bucket;
    std::string_view key;
    long range[2]{};

    void setRange(long begin, long end);

    bool hasRange() const {
        return range[0] > 0 || range[1] > 0;
    }

    std::string getRangeHeader() const;
};

class S3GetObject {
public:
    explicit S3GetObject(HttpResponse response);

    long status{};
    ByteBuffer data;
};

// S3 storage client.
class S3Client {
public:
    virtual ~S3Client() = default;
    virtual folly::Future<S3GetObjectInfo> getObjectInfo(const S3GetObjectInfoRequest& req) = 0;
    virtual folly::Future<S3GetObject> getObject(const S3GetObjectRequest& req) = 0;
};

std::unique_ptr<S3Client> createS3Client(HttpClient* httpClient, const S3ClientConfig& config);
std::time_t parseS3Time(std::string_view timeStr);

} // namespace molecula
