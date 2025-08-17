#include <iostream>
#include <memory>

#include "molecula/compiler/Compiler.hpp"
#include "molecula/http_client/HttpClient.hpp"
#include "velox/common/base/Status.h"
#include "velox/common/base/VeloxException.h"

namespace velox = facebook::velox;

int main(int argc, char** argv) {
  auto status = velox::Status::OK();
  auto compiler = std::make_unique<molecula::Compiler>();
  compiler->compile("SELECT 1");
  std::cout << "hello from mulecula v2\n";
  std::cout << "status: " << status << "\n";

  auto httpClient = molecula::createHttpClient(molecula::HttpClientParams{});
  auto request = httpClient->makeRequest("http://localhost:8080/");
  std::cout << "HTTP status: " << request->getHttpStatus() << "\n";
  std::cout << "Response body: " << request->getBody() << "\n";

  return 0;
}
