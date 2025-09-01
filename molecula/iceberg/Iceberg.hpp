#pragma once

#include "molecula/common/ByteBuffer.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// To read data for classes in this file we use "reader" pattern. Read is a class, friends with
// the target class. It reads data from Avro/JSON and populates the target class fields.
// This way we hide dependency on Avro/JSON classes and header from this file.

namespace molecula::iceberg {

class PropertyMap {
public:
    std::string_view getProperty(std::string_view key) const {
        auto it = map.find(key);
        return it != map.end() ? it->second : std::string_view{};
    }

    void setProperty(std::string_view key, const std::string_view value) {
        map[std::string(key)] = std::string(value);
    }

private:
    struct hasher {
        using is_transparent = void; // Mark as transparent
        size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };
    struct equals {
        using is_transparent = void;
        bool operator()(std::string_view a, std::string_view b) const noexcept {
            return a == b;
        }
    };
    std::unordered_map<std::string, std::string, hasher, equals> map;
};

class ManifestList {
public:
    friend class Metadata;
    friend class ManifestListReader;

    // Throws if error
    static std::unique_ptr<ManifestList> fromAvro(ByteBuffer& buffer);

private:
    PropertyMap properties;
};

class Snapshot {
public:
    friend class Metadata;
    friend class SnapshotReader;

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

    // Throws if error
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
    PropertyMap properties;
};

} // namespace molecula::iceberg
