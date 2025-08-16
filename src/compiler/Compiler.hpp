#pragma once

#include <vector>
#include <string_view>

namespace molecula {

class Compiler {
public:
  Compiler() {}
  void compile(std::string_view sql);

private:

};

}
