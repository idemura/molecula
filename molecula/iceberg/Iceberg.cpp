#include "molecula/iceberg/Iceberg.hpp"

#include <glog/logging.h>

// #define SIMDJSON_EXCEPTIONS 0
#include <simdjson.h>
// #undef SIMDJSON_EXCEPTIONS

namespace molecula {

std::unique_ptr<IcebergMetadata> IcebergMetadata::fromJson(std::string_view json) {
    simdjson::dom::parser parser;

    // Parse and iterate through an array of objects
    simdjson::padded_string abstract_json = R"(
[
{ "12345" : {"a":12.34, "b":56.78, "c": 9998877}  },
{ "12545" : {"a":11.44, "b":12.78, "c": 1111111}  }
]
)"_padded;

    for (simdjson::dom::object obj : parser.parse(abstract_json)) {
        for (const auto key_value : obj) {
            LOG(INFO) << "key: " << key_value.key << " : ";
            simdjson::dom::object innerobj = key_value.value;
            LOG(INFO) << "a: " << double(innerobj["a"]);
            LOG(INFO) << "b: " << double(innerobj["b"]);
            LOG(INFO) << "c: " << int64_t(innerobj["c"]);
        }
    }

    return std::make_unique<IcebergMetadata>();
}

} // namespace molecula
