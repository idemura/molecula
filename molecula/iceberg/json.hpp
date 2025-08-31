#pragma once

#include "molecula/common/ByteBuffer.hpp"

#include <string_view>

namespace molecula::json {

struct Object;
struct Array;

enum class Next { Continue, Stop };

/// JSON tree visitor.
/// For object, then "name" is the key. For array, it is empty.
class Visitor {
public:
    virtual ~Visitor() = default;

    virtual Next visit(std::string_view name, int64_t value);
    virtual Next visit(std::string_view name, std::string_view value);
    virtual Next visit(std::string_view name, bool value);
    virtual Next visit(std::string_view name, double value);
    virtual Next visit(std::string_view name, Object* node);
    virtual Next visit(std::string_view name, Array* node);
};

bool parse(ByteBuffer& buffer, Visitor* visitor);
Next accept(Visitor* visitor, Object* node);
Next accept(Visitor* visitor, Array* node);

} // namespace molecula::json
