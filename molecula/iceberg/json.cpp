#include "molecula/iceberg/json.hpp"

#define SIMDJSON_EXCEPTIONS 0
#define SIMDJSON_DISABLE_DEPRECATED_API 1
#include <simdjson.h>

#include <glog/logging.h>

namespace molecula::json {

Next Visitor::visit(std::string_view name, int64_t value) {
    return Next::Continue;
}

Next Visitor::visit(std::string_view name, std::string_view value) {
    return Next::Continue;
}

Next Visitor::visit(std::string_view name, bool value) {
    return Next::Continue;
}

Next Visitor::visit(std::string_view name, double value) {
    return Next::Continue;
}

Next Visitor::visit(std::string_view name, Object* node) {
    return Next::Continue;
}

Next Visitor::visit(std::string_view name, Array* node) {
    return Next::Continue;
}

struct Object {
    explicit Object(simdjson::dom::element e) {
        (void)e.get(object);
    }
    simdjson::dom::object object;
};

struct Array {
    explicit Array(simdjson::dom::element e) {
        (void)e.get(array);
    }
    simdjson::dom::array array;
};

template <typename T>
inline T jsonGetValue(simdjson::dom::element element) {
    T value{};
    (void)element.get(value);
    return value;
}

static Next
acceptDomElement(Visitor* visitor, std::string_view name, simdjson::dom::element element) {
    using simdjson::dom::element_type;

    switch (element.type()) {
        case element_type::OBJECT: {
            Object node{element};
            return visitor->visit(name, &node);
        }
        case element_type::ARRAY: {
            Array node{element};
            return visitor->visit(name, &node);
        }
        case element_type::INT64:
            return visitor->visit(name, jsonGetValue<int64_t>(element));
        case element_type::STRING:
            return visitor->visit(name, jsonGetValue<std::string_view>(element));
        case element_type::BOOL:
            return visitor->visit(name, jsonGetValue<bool>(element));
        case element_type::DOUBLE:
            return visitor->visit(name, jsonGetValue<double>(element));
        case element_type::UINT64:
            LOG(ERROR) << "Integer is out of signed 64-bit int";
            return Next::Stop;
        case element_type::NULL_VALUE:
            return Next::Continue;
    }
    // Shouldn't be here actually.
    LOG(ERROR) << "Unknown JSON element type " << element.type();
    return Next::Stop;
}

Next accept(Visitor* visitor, Object* node) {
    for (const auto [key, value] : node->object) {
        if (acceptDomElement(visitor, key, value) == Next::Stop) {
            return Next::Stop;
        }
    }
    return Next::Continue;
}

Next accept(Visitor* visitor, Array* node) {
    for (const auto item : node->array) {
        if (acceptDomElement(visitor, {}, item) == Next::Stop) {
            return Next::Stop;
        }
    }
    return Next::Continue;
}

bool parse(ByteBuffer& buffer, Visitor* visitor) {
    simdjson::dom::element root;

    simdjson::dom::parser parser;
    bool reallocIfNeeded = buffer.capacity() - buffer.size() < simdjson::SIMDJSON_PADDING;
    auto error = parser.parse(buffer.data(), buffer.size(), reallocIfNeeded).get(root);
    if (error != simdjson::SUCCESS) {
        LOG(ERROR) << simdjson::error_message(error);
        return false;
    }
    if (!root.is_object()) {
        LOG(ERROR) << "JSON is not an object";
        return false;
    }

    Object object{root};
    return accept(visitor, &object) == Next::Continue;
}

} // namespace molecula::json
