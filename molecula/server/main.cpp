#include <iostream>
#include <memory>

#include "folly/init/Init.h"
#include "molecula/compiler/Compiler.hpp"
#include "molecula/http_client/HttpClient.hpp"
#include "molecula/s3/S3Client.hpp"
#include "velox/common/base/Status.h"
#include "velox/common/base/VeloxException.h"

namespace velox = facebook::velox;

namespace molecula {

int serverMain() {
  auto status = velox::Status::OK();
  auto compiler = std::make_unique<Compiler>();
  compiler->compile("SELECT 1");
  std::cout << "hello from mulecula v2\n";
  std::cout << "status: " << status << "\n";

  auto httpClient = createHttpClientCurl(HttpClientConfig{});
  HttpRequest request{"http://localhost:8080/"};
  request.setMethod(HttpMethod::GET);
  request.addHeader("Accept: text/html");
  auto response = httpClient->makeRequest(std::move(request)).get();

  std::cout << "HTTP status: " << response.getStatus() << "\n";
  std::cout << "Response body: " << response.getBody().data() << "\n";
  std::cout << "Response headers:\n";
  for (const std::string& header : response.getHeaders()) {
    std::cout << "  " << header << "\n";
  }

  auto s3ClientConfig = S3ClientConfig{
      .httpClient = httpClient.get(),
      .endpoint = "http://localhost:9000",
      .accessKey = "minioadmin",
      .secretKey = "minioadmin"};
  auto s3Client = createS3Client(s3ClientConfig);

  return 0;
}

} // namespace molecula

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  return molecula::serverMain();
}
