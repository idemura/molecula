#include <iostream>
#include <memory>

#include "folly/init/Init.h"
#include "molecula/compiler/Compiler.hpp"
#include "molecula/http_client/HttpClient.hpp"
#include "velox/common/base/Status.h"
#include "velox/common/base/VeloxException.h"

namespace velox = facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  auto status = velox::Status::OK();
  auto compiler = std::make_unique<molecula::Compiler>();
  compiler->compile("SELECT 1");
  std::cout << "hello from mulecula v2\n";
  std::cout << "status: " << status << "\n";

  auto httpClient = molecula::createHttpClient(molecula::HttpClientParams{});
  molecula::HttpRequest request{"http://localhost:8080/"};
  request.setMethod(molecula::HttpMethod::GET);
  request.addHeader("Accept: text/html");
  auto response = httpClient->makeRequest(std::move(request)).get();

  std::cout << "HTTP status: " << response.getStatus() << "\n";
  std::cout << "Response body: " << response.getBody().data() << "\n";

  return 0;
}
