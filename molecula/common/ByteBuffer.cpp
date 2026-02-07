#include "molecula/common/ByteBuffer.hpp"

#include <cstring>
#include <utility>

namespace molecula {

size_t ByteBuffer::align(size_t size) {
    // Align size to multiple of 64. Suitable for SIMD operations.
    return (size + 63) & ~size_t{63};
}

ByteBuffer::ByteBuffer(size_t capacity) :
    data_{new char[capacity]}, size_{0}, capacity_{capacity} {}

void ByteBuffer::reserve(size_t capacity) {
    if (capacity > capacity_) {
        allocateAndCopy(capacity);
    }
}

void ByteBuffer::resize(size_t size) {
    if (size > capacity_) {
        reserve(size);
    }
    size_ = size;
}

void ByteBuffer::append(const char *data, size_t size) {
    if (data_ == nullptr || size_ + size > capacity_) {
        allocateAndCopy(std::max(capacity_ * 2, size_ + size));
    }
    std::memcpy(data_.get() + size_, data, size);
    size_ += size;
}

void ByteBuffer::append(std::string_view str) {
    append(str.data(), str.size());
}

void ByteBuffer::allocateAndCopy(size_t capacity) {
    auto alignedCapacity = align(capacity);
    auto buffer = std::make_unique_for_overwrite<char[]>(alignedCapacity);

    // Because we know that the buffer capacity is aligned, we can also align the copied size.
    // "memcpy" will use large block copy only.
    std::memcpy(buffer.get(), data_.get(), align(size_));

    data_ = std::move(buffer);
    capacity_ = alignedCapacity;
}

} // namespace molecula
