#include "molecula/iceberg/json.hpp"

#define SIMDJSON_EXCEPTIONS 0
#define SIMDJSON_DISABLE_DEPRECATED_API 1
#include <simdjson.h>

#include <glog/logging.h>

namespace json = simdjson;

namespace molecula {

JsonVisit JsonVisitor::visit(std::string_view name, int64_t value) {
    return JsonVisit::Continue;
}

JsonVisit JsonVisitor::visit(std::string_view name, std::string_view value) {
    return JsonVisit::Continue;
}

JsonVisit JsonVisitor::visit(std::string_view name, bool value) {
    return JsonVisit::Continue;
}

JsonVisit JsonVisitor::visit(std::string_view name, double value) {
    return JsonVisit::Continue;
}

JsonVisit JsonVisitor::visit(std::string_view name, JsonObject* node) {
    return JsonVisit::Continue;
}

JsonVisit JsonVisitor::visit(std::string_view name, JsonArray* node) {
    return JsonVisit::Continue;
}

struct JsonObject {
    explicit JsonObject(json::dom::element e) {
        (void)e.get(element);
    }
    json::dom::object element;
};

struct JsonArray {
    explicit JsonArray(json::dom::element e) {
        (void)e.get(element);
    }
    json::dom::array element;
};

template <typename T>
inline T jsonGetValue(json::dom::element element) {
    T value{};
    (void)element.get(value);
    return value;
}

static JsonVisit
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
        case json::dom::element_type::INT64:
            return visitor->visit(name, jsonGetValue<int64_t>(element));
        case json::dom::element_type::STRING:
            return visitor->visit(name, jsonGetValue<std::string_view>(element));
        case json::dom::element_type::BOOL:
            return visitor->visit(name, jsonGetValue<bool>(element));
        case json::dom::element_type::DOUBLE:
            return visitor->visit(name, jsonGetValue<double>(element));
        case json::dom::element_type::UINT64:
            LOG(ERROR) << "Integer is out of signed 64-bit int";
            return JsonVisit::Stop;
        case json::dom::element_type::NULL_VALUE:
            return JsonVisit::Continue;
    }
    // Shouldn't be here actually.
    LOG(ERROR) << "Unknown JSON element type " << element.type();
    return JsonVisit::Stop;
}

JsonVisit jsonAccept(JsonVisitor* visitor, JsonObject* node) {
    for (const auto [key, value] : node->element) {
        if (jsonAcceptDomElement(visitor, key, value) == JsonVisit::Stop) {
            return JsonVisit::Stop;
        }
    }
    return JsonVisit::Continue;
}

JsonVisit jsonAccept(JsonVisitor* visitor, JsonArray* node) {
    for (const auto item : node->element) {
        if (jsonAcceptDomElement(visitor, {}, item) == JsonVisit::Stop) {
            return JsonVisit::Stop;
        }
    }
    return JsonVisit::Continue;
}

bool jsonParse(ByteBuffer& buffer, JsonVisitor* visitor) {
    json::dom::element root;

    json::dom::parser parser;
    bool reallocIfNeeded = buffer.capacity() - buffer.size() < json::SIMDJSON_PADDING;
    auto error = parser.parse(buffer.data(), buffer.size(), reallocIfNeeded).get(root);
    if (error != json::SUCCESS) {
        LOG(ERROR) << json::error_message(error);
        return false;
    }
    if (!root.is_object()) {
        LOG(ERROR) << "JSON is not an object";
        return false;
    }

    JsonObject object{root};
    return jsonAccept(visitor, &object) == JsonVisit::Continue;
}

} // namespace molecula
