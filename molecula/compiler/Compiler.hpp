#pragma once

#include <string_view>
#include <vector>

namespace molecula {

class Compiler {
 public:
  Compiler() {}
  void compile(std::string_view sql);

 private:
};

} // namespace molecula
