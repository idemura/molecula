#include "molecula/server/Server.hpp"

#include "molecula/iceberg/Iceberg.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(s3_endpoint, "", "S3 endpoint");
DEFINE_string(s3_access_key, "", "S3 access key");
DEFINE_string(s3_secret_key, "", "S3 secret key");
DEFINE_string(s3_region, "us-east-1", "S3 region");

namespace molecula {

std::unique_ptr<Server> createServer() {
    auto httpClient = createHttpClientCurl(HttpClientConfig{});
    if (!httpClient) {
        return nullptr;
    }
    auto s3ClientConfig = S3ClientConfig{
            .endpoint = FLAGS_s3_endpoint,
            .accessKey = FLAGS_s3_access_key,
            .secretKey = FLAGS_s3_secret_key,
            .region = FLAGS_s3_region};
    auto s3Client = createS3Client(httpClient.get(), s3ClientConfig);
    if (!s3Client) {
        return nullptr;
    }
    return std::make_unique<Server>(std::move(httpClient), std::move(s3Client));
}

Server::Server(std::unique_ptr<HttpClient> httpClient, std::unique_ptr<S3Client> s3Client) :
    httpClient{std::move(httpClient)}, s3Client{std::move(s3Client)} {}

void Server::start() {
    // Start the server
}

void Server::stop() {
    // Stop the server
}

void Server::testIceberg() {
    LOG(INFO) << "Testing Iceberg...";
    S3GetObjectRequest req{"datalake", "db/testtbl/metadata/v2.metadata.json"};
    auto response = s3Client->getObject(req).get();
    if (response.status != 200) {
        LOG(ERROR) << "Failed to get Iceberg metadata, status: " << response.status;
        return;
    }

    auto metadata = IcebergMetadata::fromJson(response.data);
    LOG(INFO) << "Table UUID: " << metadata->getUuid();
    LOG(INFO) << "Table location: " << metadata->getLocation();
}

} // namespace molecula
