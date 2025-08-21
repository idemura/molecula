#include "molecula/s3/S3ClientImpl.hpp"

#include <glog/logging.h>
#include <charconv>

namespace molecula {
std::unique_ptr<S3Client> createS3Client(HttpClient* httpClient, const S3ClientConfig& config) {
    return std::make_unique<S3ClientImpl>(httpClient, config);
}

S3ClientImpl::S3ClientImpl(HttpClient* httpClient, const S3ClientConfig& config) :
    httpClient{httpClient},
    endpoint{config.endpoint},
    signer{config.accessKey, config.secretKey, config.region},
    config{config} {}

void S3ClientImpl::setObject(S3Request& request, std::string_view bucket, std::string_view key)
        const {
    if (config.pathStyle) {
        request.setHost(endpoint.host());
        request.setPath(std::string{"/"}.append(bucket).append("/").append(key));
    } else {
        request.setHost(std::string{bucket}.append(".").append(endpoint.host()));
        request.setPath(std::string{"/"}.append(key));
    }
}

HttpRequest S3ClientImpl::createHttpRequest(S3Request& request) const {
    std::string url;
    url.append(endpoint.scheme());
    url.append("://");
    url.append(request.getHost());
    if (endpoint.port() > 0) {
        url.append(":");
        char buf[16];
        auto [ptr, ec] = std::to_chars(std::begin(buf), std::end(buf), endpoint.port());
        url.append(buf, ptr - buf);
    }
    url.append(request.getPath());
    if (!request.getQuery().empty()) {
        url.append("?");
        url.append(request.getQuery());
    }

    HttpRequest httpRequest;
    httpRequest.url = std::move(url);
    httpRequest.method = request.method;
    for (std::string& header : request.headers.span()) {
        httpRequest.headers.add(std::move(header));
    }
    return httpRequest;
}

folly::Future<S3GetObjectInfo> S3ClientImpl::getObjectInfo(const S3GetObjectInfoRequest& req) {
    S3Time time;

    // Prepare S3 request and sign it
    S3Request s3Req;
    s3Req.method = HttpMethod::HEAD;
    setObject(s3Req, req.bucket, req.key);
    signer.sign(s3Req, time);

    // Make HTTP request
    HttpRequest request = createHttpRequest(s3Req);
    return httpClient->makeRequest(std::move(request)).thenValue([](HttpResponse response) {
        return S3GetObjectInfo{std::move(response)};
    });
}

folly::Future<S3GetObject> S3ClientImpl::getObject(const S3GetObjectRequest& req) {
    S3Time time;

    // Prepare S3 request and sign it
    S3Request s3Req;
    s3Req.method = HttpMethod::GET;
    setObject(s3Req, req.bucket, req.key);
    if (req.hasRange()) {
        s3Req.headers.add(req.getRangeHeader());
    }
    signer.sign(s3Req, time);

    // Make HTTP request
    HttpRequest request = createHttpRequest(s3Req);
    return httpClient->makeRequest(std::move(request)).thenValue([](HttpResponse response) {
        return S3GetObject{std::move(response)};
    });
}

} // namespace molecula
