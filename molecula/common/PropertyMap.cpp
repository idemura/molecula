#include "molecula/common/PropertyMap.hpp"

namespace molecula {

std::string_view PropertyMap::getProperty(std::string_view key) const noexcept {
    auto it = map.find(key);
    return it != map.end() ? it->second : std::string_view{};
}

void PropertyMap::setProperty(std::string_view key, std::string_view value) noexcept {
    map[std::string(key)] = std::string(value);
}

} // namespace molecula
