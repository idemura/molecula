#include "molecula/iceberg/Iceberg.hpp"

#include <glog/logging.h>

// #define SIMDJSON_EXCEPTIONS 0
#include <simdjson.h>
// #undef SIMDJSON_EXCEPTIONS

namespace molecula {

std::unique_ptr<IcebergMetadata> IcebergMetadata::fromJson(ByteBuffer& buffer) {
    try {
        bool realloc_if_needed = buffer.capacity() - buffer.size() < simdjson::SIMDJSON_PADDING;
        simdjson::dom::parser parser;
        simdjson::dom::element root = parser.parse(buffer.data(), buffer.size(), realloc_if_needed);
        LOG(INFO) << "table-uuid: " << root["table-uuid"];
        // for (simdjson::dom::object obj : root) {
        //     for (const simdjson::dom::key_value_pair kv : obj) {
        //         LOG(INFO) << "key: " << kv.key << "";
        //         simdjson::dom::object innerobj = kv.value;
        //         LOG(INFO) << "a: " << double(innerobj["a"]);
        //         LOG(INFO) << "b: " << double(innerobj["b"]);
        //         LOG(INFO) << "c: " << int64_t(innerobj["c"]);
        //     }
        // }
    } catch (const simdjson::simdjson_error& e) {
        LOG(ERROR) << "JSON parsing error " << e.what();
        return nullptr;
    }
    return std::make_unique<IcebergMetadata>();
}

} // namespace molecula
