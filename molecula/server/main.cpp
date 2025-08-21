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
  HttpRequest request;
  request.url = "http://localhost:8080/";
  request.method = HttpMethod::GET;
  request.headers.add("Accept: text/html");
  auto response = httpClient->makeRequest(std::move(request)).get();

  LOG(INFO) << "HTTP status: " << response.status;
  LOG(INFO) << "Response body: " << response.body.view();
  LOG(INFO) << "Response headers:\n";
  for (const std::string& header : response.headers.span()) {
    LOG(INFO) << "  " << header;
  }

  auto s3ClientConfig = S3ClientConfig{
      .endpoint = "http://localhost:9000",
      .accessKey = "gQkIXMhx6R0crumAU6vg",
      .secretKey = "cOqMq4abd5IUiM9q1pg4WHFB0TO12A4BpfdUaG3A",
      .region = "us-east-1"};
  auto s3Client = createS3Client(httpClient.get(), s3ClientConfig);

  // S3GetObjectRequest req{"datalake", "poem.txt"};
  {
    S3GetObjectRequest req{"datalake", "my/kitty/elsa"};
    auto res = s3Client->getObject(req).get();
    LOG(INFO) << "S3 GET Object status: " << res.status;
    LOG(INFO) << "S3 GET Object data: " << res.data.view();
  }
  {
    S3GetObjectRequest req{"datalake", "my/kitty/elsa"};
    req.setRange(0, 5);
    auto res = s3Client->getObject(req).get();
    LOG(INFO) << "S3 GET Object status: " << res.status;
    LOG(INFO) << "S3 GET Object data: " << res.data.view();
  }
  {
    S3GetObjectInfoRequest req{"datalake", "my/kitty/elsa"};
    auto res = s3Client->getObjectInfo(req).get();
    LOG(INFO) << "S3 GET Object Info status: " << res.status;
    LOG(INFO) << "S3 GET Object Info etag: " << res.etag;
    LOG(INFO) << "S3 GET Object Info size: " << res.size;
    LOG(INFO) << "S3 GET Object Info last modified: " << res.lastModified;
  }
  return 0;
}

} // namespace molecula

int main(int argc, char** argv) {
  // Initializes glog inside
  folly::Init init(&argc, &argv);

  return molecula::serverMain();
}
