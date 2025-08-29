#include "molecula/iceberg/json.hpp"

#define SIMDJSON_EXCEPTIONS 0
#define SIMDJSON_DISABLE_DEPRECATED_API 1
#include <simdjson.h>

#include <glog/logging.h>

namespace json = simdjson;

namespace molecula {

bool JsonVisitor::visit(std::string_view name, int64_t value) {
    return true;
}

bool JsonVisitor::visit(std::string_view name, std::string_view value) {
    return true;
}

bool JsonVisitor::visit(std::string_view name, bool value) {
    return true;
}

bool JsonVisitor::visit(std::string_view name, JsonObject* node) {
    return true;
}

bool JsonVisitor::visit(std::string_view name, JsonArray* node) {
    return true;
}

struct JsonObject {
    explicit JsonObject(json::dom::element e) {
        CHECK(e.get(element) == json::SUCCESS);
    }
    json::dom::object element;
};

struct JsonArray {
    explicit JsonArray(json::dom::element e) {
        CHECK(e.get(element) == json::SUCCESS);
    }
    json::dom::array element;
};

static bool
jsonAcceptDomElement(JsonVisitor* visitor, std::string_view name, json::dom::element element) {
    switch (element.type()) {
        case json::dom::element_type::OBJECT: {
            JsonObject node{element};
            return visitor->visit(name, &node);
        }
        case json::dom::element_type::ARRAY: {
            JsonArray node{element};
            return visitor->visit(name, &node);
        }
        case json::dom::element_type::INT64: {
            int64_t v{};
            (void)element.get(v);
            return visitor->visit(name, v);
        }
        case json::dom::element_type::STRING: {
            std::string_view v;
            (void)element.get(v);
            return visitor->visit(name, v);
        }
        case json::dom::element_type::BOOL: {
            bool v{};
            (void)element.get(v);
            return visitor->visit(name, v);
        }
        case json::dom::element_type::NULL_VALUE:
            return true;
        default:
            LOG(ERROR) << "Unsupported type " << element.type();
    }
    return false;
}

bool jsonAccept(JsonVisitor* visitor, JsonObject* node) {
    for (const auto [key, value] : node->element) {
        if (!jsonAcceptDomElement(visitor, key, value)) {
            return false;
        }
    }
    return true;
}

bool jsonAccept(JsonVisitor* visitor, JsonArray* node) {
    for (const auto item : node->element) {
        if (!jsonAcceptDomElement(visitor, {}, item)) {
            return false;
        }
    }
    return true;
}

bool jsonParse(ByteBuffer& buffer, JsonVisitor* visitor) {
    json::dom::element root;

    json::dom::parser parser;
    bool reallocIfNeeded = buffer.capacity() - buffer.size() < json::SIMDJSON_PADDING;
    auto error = parser.parse(buffer.data(), buffer.size(), reallocIfNeeded).get(root);
    if (error != json::SUCCESS) {
        LOG(INFO) << json::error_message(error);
        return false;
    }
    if (!root.is_object()) {
        LOG(INFO) << "JSON is not an object";
        return false;
    }

    JsonObject object{root};
    return jsonAccept(visitor, &object);
}

} // namespace molecula
