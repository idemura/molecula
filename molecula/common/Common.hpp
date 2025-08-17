#pragma once

#include <cstring>
#include <iosfwd>
#include <memory>
#include <span>
#include <string_view>

namespace molecula {
// Non-modifiable string. Can't append, but can modify contents. Smaller than std::string.
class String {
public:
  String() = default;
  String(const char* str) : String{std::string_view{str}} {}
  String(std::string_view str) {
    copy(str);
  }
  String(const String& other) : String{other.view()} {}
  String(String&& other) noexcept : size_{other.size_}, data_{std::move(other.data_)} {
    other.size_ = 0;
  }

  String& operator=(const String& other) {
    if (this != &other) {
      copy(other.view());
    }
    return *this;
  }

  String& operator=(String&& other) noexcept {
    size_ = other.size_;
    data_ = std::move(other.data_);
    other.size_ = 0;
    return *this;
  }

  std::string_view view() const noexcept {
    return std::string_view{data_.get(), size_};
  }

  // Returns a null-terminated C-style string.
  char* data() noexcept {
    return data_.get();
  }

  // Returns a null-terminated C-style string.
  const char* data() const noexcept {
    return data_.get();
  }

  char* begin() noexcept {
    return data_.get();
  }

  const char* begin() const noexcept {
    return data_.get();
  }

  char* end() noexcept {
    return data_.get() + size_;
  }

  const char* end() const noexcept {
    return data_.get() + size_;
  }

  size_t size() const noexcept {
    return size_;
  }

  bool empty() const noexcept {
    return size_ == 0;
  }

  char& operator[](size_t index) {
    return data_.get()[index];
  }

  char operator[](size_t index) const {
    return data_.get()[index];
  }

private:
  void copy(std::string_view str);

  size_t size_ = 0;
  std::unique_ptr<char[]> data_;
};

std::ostream& operator<<(std::ostream& os, const String& str);

} // namespace molecula
