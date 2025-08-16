#include <iostream>
#include <memory>

#include "compiler/Compiler.hpp"
#include "velox/common/base/Status.h"
#include "velox/common/base/VeloxException.h"

namespace velox = facebook::velox;

int main(int argc, char** argv) {
  auto status = velox::Status::OK();
  auto compiler = std::make_unique<molecula::Compiler>();
  compiler->compile("SELECT 1");
  std::cout << "hello from mulecula v2\n";
  std::cout << "status: " << status << "\n";
  return 0;
}
