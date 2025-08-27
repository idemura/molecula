#pragma once

#include "molecula/http_client/HttpClient.hpp"
#include "molecula/s3/S3Client.hpp"

#include <memory>

namespace molecula {

// SQL server.
class Server {
public:
    Server(std::unique_ptr<HttpClient> httpClient, std::unique_ptr<S3Client> s3Client);

    void start();
    void stop();
    void testIceberg();

private:
    std::unique_ptr<HttpClient> httpClient;
    std::unique_ptr<S3Client> s3Client;
};

std::unique_ptr<Server> createServer();

} // namespace molecula
