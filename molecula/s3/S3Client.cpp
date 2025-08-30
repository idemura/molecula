#include "molecula/s3/S3Client.hpp"

#include <glog/logging.h>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace molecula {

S3Id S3Id::fromString(std::string uri) {
    size_t colonPos = uri.find(':');
    if (colonPos == std::string::npos) {
        return S3Id{};
    }
    if (uri.size() < colonPos + 3) {
        return S3Id{};
    }
    if (!(uri[colonPos + 1] == '/' && uri[colonPos + 2] == '/')) {
        return S3Id{};
    }
    size_t bucketStart = colonPos + 3;
    size_t bucketEnd = uri.find('/', bucketStart);
    if (bucketEnd == std::string::npos || bucketEnd == bucketStart) {
        return S3Id{};
    }
    return S3Id{std::move(uri), bucketStart, bucketEnd};
}

S3Id S3Id::fromPieces(std::string_view bucket, std::string_view key) {
    std::string uri{"s3://"};
    uri.append(bucket);
    uri.append("/");
    uri.append(key);
    return S3Id{std::move(uri), 5, 5 + bucket.size()};
}

S3GetObjectInfo::S3GetObjectInfo(HttpResponse response) : status{response.status} {
    if (is2xx(status)) {
        etag = response.headers.get("etag");
        size = response.headers.getContentLength();

        auto lastModifiedStr = response.headers.get("last-modified");
        if (!lastModifiedStr.empty()) {
            std::tm tm{};
            std::istringstream ss{std::string{lastModifiedStr}};
            ss >> std::get_time(&tm, "%a, %d %b %Y %H:%M:%S GMT");
            lastModified = ::timegm(&tm);
        }
    } else {
        LOG(ERROR) << "Failed GetObjectInfo: " << status << "\n" << response.body.view();
    }
}

S3GetObject::S3GetObject(HttpResponse response) : status{response.status} {
    if (is2xx(status)) {
        data = std::move(response.body);
    } else {
        LOG(ERROR) << "Failed GetObject: " << status << "\n" << response.body.view();
    }
}

void S3GetObjectRequest::setRange(long begin, long end) {
    CHECK(begin >= 0 && end >= begin) << "Invalid range: [" << begin << ", " << end << "]";
    range[0] = begin;
    range[1] = end;
}

std::string S3GetObjectRequest::getRangeHeader() const {
    char buffer[80];
    size_t n = std::snprintf(buffer, sizeof(buffer), "range:bytes=%ld-%ld", range[0], range[1]);
    return std::string(buffer, n);
}

} // namespace molecula
