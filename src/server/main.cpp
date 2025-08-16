#include <iostream>
#include <memory>

#include "compiler/Compiler.hpp"

int main(int argc, char **argv) {
    auto compiler = std::make_unique<molecula::Compiler>();
    compiler->compile("SELECT 1");
    std::cout << "hello from mulecula\n";
    return 0;
}
