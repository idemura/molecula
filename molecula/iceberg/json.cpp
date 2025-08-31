#include "molecula/iceberg/json.hpp"

#include <glog/logging.h>

namespace molecula::json {

bool parse(ByteBuffer& buffer, dom::document& doc) {
    json::dom::parser parser;
    bool reallocIfNeeded = buffer.capacity() - buffer.size() < SIMDJSON_PADDING;
    auto error =
            parser.parse_into_document(doc, buffer.data(), buffer.size(), reallocIfNeeded).error();
    if (error != SUCCESS) {
        LOG(ERROR) << error_message(error);
        return false;
    }
    return true;
}

} // namespace molecula::json
