#include "molecula/iceberg/json.hpp"

#include <glog/logging.h>

namespace molecula::json {

bool parse(std::string_view data, size_t capacity, dom::document &doc) {
    json::dom::parser parser;
    bool reallocIfNeeded = capacity - data.size() < SIMDJSON_PADDING;
    json::error_code error =
            parser.parse_into_document(doc, data.data(), data.size(), reallocIfNeeded).error();
    if (error != SUCCESS) {
        LOG(ERROR) << error_message(error);
        return false;
    }
    return true;
}

} // namespace molecula::json
