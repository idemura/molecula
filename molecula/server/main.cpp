#include <glog/logging.h>

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
  LOG(INFO) << "Status: " << status;

  auto httpClient = createHttpClientCurl(HttpClientConfig{});
  HttpRequest request{"http://localhost:8080/"};
  request.setMethod(HttpMethod::GET);
  request.addHeader("Accept: text/html");
  auto response = httpClient->makeRequest(std::move(request)).get();

  LOG(INFO) << "HTTP status: " << response.getStatus();
  LOG(INFO) << "Response body: " << response.getBody().view();
  LOG(INFO) << "Response headers:\n";
  for (const std::string& header : response.getHeaders()) {
    LOG(INFO) << "  " << header;
  }

  auto s3ClientConfig = S3ClientConfig{
      .endpoint = "http://localhost:9000", .accessKey = "minioadmin", .secretKey = "minioadmin"};
  auto s3Client = createS3Client(httpClient.get(), s3ClientConfig);

  S3GetObjectReq req{"datalake", "poem.txt"};
  auto res = s3Client->getObject(req).get();
  // LOG(INFO) << "S3 GET Object status: " << res.status;
  // LOG(INFO) << "S3 GET Object data: " << res.data.data();

  return 0;
}

} // namespace molecula

int main(int argc, char** argv) {
  // Initializes glog inside
  folly::Init init(&argc, &argv);

  return molecula::serverMain();
}
