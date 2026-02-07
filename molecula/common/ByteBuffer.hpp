#pragma once

#include <memory>
#include <span>
#include <string_view>

namespace molecula {

using ByteSpan = std::span<const char>;

// Owning and growing buffer of chars.
class ByteBuffer {
public:
    static size_t align(size_t size);

    ByteBuffer() = default;
    explicit ByteBuffer(size_t capacity);

    ByteBuffer(const ByteBuffer &) = delete;
    ByteBuffer &operator=(const ByteBuffer &) = delete;

    ByteBuffer(ByteBuffer &&other) noexcept = default;
    ByteBuffer &operator=(ByteBuffer &&) noexcept = default;

    void reserve(size_t capacity);
    void resize(size_t size);
    void append(const char *data, size_t size);
    void append(std::string_view str);

    void clear() {
        size_ = 0;
    }

    size_t size() const {
        return size_;
    }

    size_t capacity() const {
        return capacity_;
    }

    char *data() {
        return data_.get();
    }

    std::span<char> span() {
        return std::span<char>{data_.get(), size_};
    }

    std::string_view view() const {
        return std::string_view{data_.get(), size_};
    }

private:
    void allocateAndCopy(size_t capacity);

    std::unique_ptr<char[]> data_;
    size_t size_{};
    size_t capacity_{};
};

} // namespace molecula
