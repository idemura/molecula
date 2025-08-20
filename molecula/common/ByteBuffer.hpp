#pragma once

#include <memory>
#include <span>
#include <string_view>

namespace molecula {

class ByteBuffer {
public:
  ByteBuffer() = default;
  explicit ByteBuffer(size_t capacity);

  ByteBuffer(const ByteBuffer&) = delete;
  ByteBuffer& operator=(const ByteBuffer&) = delete;

  ByteBuffer(ByteBuffer&& other) noexcept = default;
  ByteBuffer& operator=(ByteBuffer&&) = default;

  void reserve(size_t capacity);
  void append(const char* data, size_t size);
  void append(std::string_view str);

  void clear() {
    size_ = 0;
  }

  size_t size() const noexcept {
    return size_;
  }

  char* data() {
    return data_.get();
  }

  std::span<char> span() {
    return std::span<char>{data_.get(), size_};
  }

  std::string_view view() const noexcept {
    return std::string_view{data_.get(), size_};
  }

private:
  static size_t align(size_t size);
  void allocate(size_t capacity);

  std::unique_ptr<char[]> data_;
  size_t size_ = 0;
  size_t capacity_ = 0;
};

} // namespace molecula
