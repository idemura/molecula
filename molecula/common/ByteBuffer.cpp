#include "molecula/common/ByteBuffer.hpp"

#include <cstring>
#include <utility>

namespace molecula {

ByteBuffer::ByteBuffer(size_t capacity) :
    data_{new char[capacity]}, size_{0}, capacity_{capacity} {}

size_t ByteBuffer::align(size_t size) {
    // Align size to multiple of 16
    return (size + 15) & ~size_t{15};
}

void ByteBuffer::reserve(size_t capacity) {
    if (capacity > capacity_) {
        allocate(align(capacity));
    }
}

void ByteBuffer::resize(size_t size) {
    if (size > capacity_) {
        reserve(size);
    }
    size_ = size;
}

void ByteBuffer::append(const char* data, size_t size) {
    if (data_ == nullptr || size_ + size > capacity_) {
        allocate(std::max(capacity_ * 2, align(size_ + size)));
    }
    std::memcpy(data_.get() + size_, data, size);
    size_ += size;
}

void ByteBuffer::append(std::string_view str) {
    append(str.data(), str.size());
}

void ByteBuffer::allocate(size_t capacity) {
    auto newData = std::make_unique_for_overwrite<char[]>(capacity);
    std::memcpy(newData.get(), data_.get(), size_);
    data_ = std::move(newData);
    capacity_ = capacity;
}

} // namespace molecula
