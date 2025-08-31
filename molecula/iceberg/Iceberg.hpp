#pragma once

#include "molecula/common/ByteBuffer.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace molecula::iceberg {

class ManifestList {
public:
    static std::unique_ptr<ManifestList> fromAvro(ByteBuffer& buffer);

private:
};

class Snapshot {
public:
    friend class SnapshotReader;
    friend class Metadata;

    Snapshot() = default;

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
class Metadata {
public:
    friend class MetadataReader;

    static std::unique_ptr<Metadata> fromJson(ByteBuffer& buffer);

    Metadata() = default;

    std::string_view getUuid() const {
        return uuid;
    }

    std::string_view getLocation() const {
        return location;
    }

    Snapshot* findCurrentSnapshot();

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
