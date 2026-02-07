#pragma once

#include "molecula/common/types.hpp"

#define SIMDJSON_EXCEPTIONS 0
#define SIMDJSON_DISABLE_DEPRECATED_API 1
#include <simdjson.h>

#include <chrono>
#include <string>
#include <string_view>

namespace molecula::json {

using namespace simdjson;

template <typename S, typename T>
bool get_value_convert(dom::element element, T &out) {
    S v{};
    if (element.get(v) != SUCCESS) {
        return false;
    }
    out = T(v);
    return true;
}

inline bool get_value(dom::element element, int64_t &out) {
    return element.get(out) == SUCCESS;
}

inline bool get_value(dom::element element, int32_t &out) {
    return get_value_convert<int64_t, int32_t>(element, out);
}

inline bool get_value(dom::element element, std::string_view &out) {
    return element.get(out) == SUCCESS;
}

inline bool get_value(dom::element element, std::string &out) {
    return get_value_convert<std::string_view, std::string>(element, out);
}

inline bool get_value(dom::element element, bool &out) {
    return element.get(out) == SUCCESS;
}

inline bool get_value(dom::element element, float64 &out) {
    return element.get(out) == SUCCESS;
}

inline bool get_value(dom::element element, float32 &out) {
    return get_value_convert<float64, float32>(element, out);
}

inline bool get_value(dom::element element, std::chrono::milliseconds &out) {
    return get_value_convert<int64_t, std::chrono::milliseconds>(element, out);
}

bool parse(std::string_view data, size_t capacity, dom::document &doc);

} // namespace molecula::json
