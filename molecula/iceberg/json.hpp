#pragma once

#include "molecula/common/ByteBuffer.hpp"

#include <string_view>

namespace molecula {

struct JsonObject;
struct JsonArray;

enum class JsonVisit { Continue, Stop };

/// Json tree visitor.
/// For object, then "name" is the key. For array, it is empty.
class JsonVisitor {
public:
    virtual ~JsonVisitor() = default;

    virtual JsonVisit visit(std::string_view name, int64_t value);
    virtual JsonVisit visit(std::string_view name, std::string_view value);
    virtual JsonVisit visit(std::string_view name, bool value);
    virtual JsonVisit visit(std::string_view name, JsonObject* node);
    virtual JsonVisit visit(std::string_view name, JsonArray* node);
};

bool jsonParse(ByteBuffer& buffer, JsonVisitor* visitor);
JsonVisit jsonAccept(JsonVisitor* visitor, JsonObject* node);
JsonVisit jsonAccept(JsonVisitor* visitor, JsonArray* node);

} // namespace molecula
