#pragma once

#include "molecula/common/ByteBuffer.hpp"

#include <memory>
#include <string>
#include <string_view>

namespace molecula {

struct JsonObject;
struct JsonArray;

/// Json tree visitor.
/// If we visit an object, then "name" is the key. If this is an array, then "name" is is empty.
class JsonVisitor {
public:
    virtual ~JsonVisitor() = default;

    virtual bool visit(std::string_view name, int64_t value);
    virtual bool visit(std::string_view name, std::string_view value);
    virtual bool visit(std::string_view name, bool value);
    virtual bool visit(std::string_view name, JsonObject* node);
    virtual bool visit(std::string_view name, JsonArray* node);
};

bool jsonParse(ByteBuffer& buffer, JsonVisitor* visitor);
bool jsonAccept(JsonVisitor* visitor, JsonObject* node);
bool jsonAccept(JsonVisitor* visitor, JsonArray* node);

} // namespace molecula
