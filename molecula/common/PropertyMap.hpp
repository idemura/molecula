#pragma once

#include <string>
#include <string_view>
#include <unordered_map>

namespace molecula {

class PropertyMap {
public:
    void setProperty(std::string_view key, std::string_view value) noexcept;
    std::string_view getProperty(std::string_view key) const noexcept;

private:
    struct hasher {
        using is_transparent = void; // Mark as transparent
        size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };
    struct equals {
        using is_transparent = void;
        bool operator()(std::string_view a, std::string_view b) const noexcept {
            return a == b;
        }
    };
    std::unordered_map<std::string, std::string, hasher, equals> map;
};

} // namespace molecula
