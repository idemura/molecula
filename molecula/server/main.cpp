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

  auto httpClient = molecula::createHttpClient();
  httpClient->makeRequest("localhost", 8080, "/");

  return 0;
}
