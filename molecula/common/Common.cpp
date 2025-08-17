#include "molecula/common/Common.hpp"

#include <cstring>
#include <iostream>

namespace molecula {

void String::copy(std::string_view str) {
  size_ = str.size();
  data_.reset(new char[size_ + 1]);
  std::memcpy(data_.get(), str.data(), size_);
  data_[size_] = '\0'; // Null-terminate
}

std::ostream& operator<<(std::ostream& os, const String& str) {
  os << str.view();
  return os;
}

} // namespace molecula
