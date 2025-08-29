#pragma once

#include "molecula/common/ByteBuffer.hpp"
#include "molecula/iceberg/json.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace molecula {

class IcebergSnapshot {
public:
    int64_t id{};
    std::chrono::milliseconds timestamp;
    std::string manifestList;
    int64_t schemaId{};

    IcebergSnapshot() = default;
};

// Iceberg table metadata.
class IcebergMetadata : public JsonVisitor {
public:
    static std::unique_ptr<IcebergMetadata> fromJson(ByteBuffer& buffer);

    IcebergMetadata() = default;

    std::string_view getUuid() const {
        return uuid;
    }

    std::string_view getLocation() const {
        return location;
    }

    bool visit(std::string_view name, int64_t value) override;
    bool visit(std::string_view name, std::string_view value) override;
    bool visit(std::string_view name, bool value) override;
    bool visit(std::string_view name, JsonObject* node) override;
    bool visit(std::string_view name, JsonArray* node) override;

private:
    std::string uuid;
    std::string location;
    int64_t currentSchemaId{};
    int64_t currentSnapshotId{};
    int64_t lastColumnId{};
    int64_t lastSequenceNumber{};
    int64_t lastUpdatedMillis{};
    std::unordered_map<std::string, std::string> properties;
    std::unordered_map<int64_t, std::unique_ptr<IcebergSnapshot>> snapshots;
};

} // namespace molecula
