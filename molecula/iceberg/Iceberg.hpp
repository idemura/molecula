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

class IcebergSnapshot : public JsonVisitor {
public:
    friend class IcebergMetadata;

    IcebergSnapshot() = default;
    JsonVisit visit(std::string_view name, int64_t value) override;
    JsonVisit visit(std::string_view name, std::string_view value) override;

    std::string_view getManifestList() const {
        return manifestList;
    }

private:
    int64_t id{};
    int64_t schemaId{};
    int64_t sequenceNumber{};
    std::chrono::milliseconds timestamp;
    std::string manifestList;
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

    IcebergSnapshot* findCurrentSnapshot();

    JsonVisit visit(std::string_view name, int64_t value) override;
    JsonVisit visit(std::string_view name, std::string_view value) override;
    JsonVisit visit(std::string_view name, bool value) override;
    JsonVisit visit(std::string_view name, JsonObject* node) override;
    JsonVisit visit(std::string_view name, JsonArray* node) override;

private:
    std::string uuid;
    std::string location;
    int64_t currentSchemaId{};
    int64_t currentSnapshotId{};
    int64_t lastColumnId{};
    int64_t lastSequenceNumber{};
    std::chrono::milliseconds lastUpdated;
    std::vector<IcebergSnapshot> snapshots;
    std::unordered_map<std::string, std::string> properties;
};

} // namespace molecula
