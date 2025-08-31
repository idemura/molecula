#pragma once

#include "folly/Range.h"
#include "molecula/common/ByteBuffer.hpp"
#include "molecula/iceberg/json.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace molecula::iceberg {

// Data reader from Avro file. If reached end of data, all read operations will return
// default values.
class AvroReader {
public:
    AvroReader(const char* data, size_t size) : sp{data, size} {}

    size_t remaining() const {
        return sp.size();
    }

    char readByte();
    int64_t readInt();
    std::string_view readString(size_t length);
    // Reads string length from data first.
    std::string_view readString();

private:
    folly::StringPiece sp;
};

class ManifestList {
public:
    static std::unique_ptr<ManifestList> fromAvro(ByteBuffer& buffer);

private:
};

class Snapshot : public json::Visitor {
public:
    friend class Metadata;

    Snapshot() = default;
    json::Next visit(std::string_view name, int64_t value) override;
    json::Next visit(std::string_view name, std::string_view value) override;

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
class Metadata : public json::Visitor {
public:
    static std::unique_ptr<Metadata> fromJson(ByteBuffer& buffer);

    Metadata() = default;

    std::string_view getUuid() const {
        return uuid;
    }

    std::string_view getLocation() const {
        return location;
    }

    Snapshot* findCurrentSnapshot();

    json::Next visit(std::string_view name, int64_t value) override;
    json::Next visit(std::string_view name, std::string_view value) override;
    json::Next visit(std::string_view name, bool value) override;
    json::Next visit(std::string_view name, json::Object* node) override;
    json::Next visit(std::string_view name, json::Array* node) override;

private:
    std::string uuid;
    std::string location;
    int64_t currentSchemaId{};
    int64_t currentSnapshotId{};
    int64_t lastColumnId{};
    int64_t lastSequenceNumber{};
    std::chrono::milliseconds lastUpdated;
    std::vector<Snapshot> snapshots;
    std::unordered_map<std::string, std::string> properties;
};

} // namespace molecula::iceberg
