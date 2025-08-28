#pragma once

#include <glog/logging.h>

#define SIMDJSON_EXCEPTIONS 0
#include <simdjson.h>

#include <string>
#include <string_view>
#include <utility>

namespace json {
using namespace simdjson;

inline void check(error_code e) {
    CHECK(e == SUCCESS) << error_message(e);
}

inline void get(simdjson_result<dom::element> e, int64_t& value) {
    check(e.get(value));
}

inline void get(simdjson_result<dom::element> e, std::string_view& value) {
    check(e.get(value));
}

inline void get(simdjson_result<dom::element> e, std::string& value) {
    std::string_view s;
    get(e, s);
    value = s;
}

inline bool get_opt(simdjson_result<dom::element> e, int64_t& value) {
    return e.get(value) == SUCCESS;
}

inline bool get_opt(simdjson_result<dom::element> e, std::string_view& value) {
    return e.get(value) == SUCCESS;
}

inline bool get_opt(simdjson_result<dom::element> e, std::string& value) {
    std::string_view s;
    if (!get_opt(e, s)) {
        return false;
    }
    value = s;
    return true;
}

} // namespace json
