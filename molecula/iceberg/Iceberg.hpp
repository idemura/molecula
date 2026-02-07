#pragma once

#include "molecula/common/PropertyMap.hpp"

#include <chrono>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// To read data for classes in this file we use "reader" pattern. Read is a class, friends with
// the target class. It reads data from Avro/JSON and populates the target class fields.
// This way we hide dependency on Avro/JSON classes and header from this file.

namespace molecula::iceberg {

enum class ManifestContent { Data, Deletes };
enum class DataFileContent { Data, PositionDeletes, EqualityDeletes };

class ManifestListEntry {
public:
    std::string manifestPath;
    int64_t manifestLength{};
    ManifestContent content{};
    int64_t sequenceNumber{};
};

class ManifestList {
public:
    friend class Metadata;
    friend class ManifestListReader;

    // Throws if error
    static std::unique_ptr<ManifestList> fromAvro(std::string_view data);

    std::span<ManifestListEntry> getManifests() {
        return std::span{manifests};
    }

private:
    PropertyMap properties;
    std::vector<ManifestListEntry> manifests;
};

class ManifestEntry {
public:
    int64_t sequenceNumber{};
    int64_t fileSequenceNumber{};
    DataFileContent content{};
    std::string filePath;
    std::string fileFormat;
    int64_t fileSize{};
    int64_t recordCount{};
};

class Manifest {
public:
    friend class Metadata;
    friend class ManifestReader;

    // Throws if error
    static std::unique_ptr<Manifest> fromAvro(std::string_view data);

private:
    PropertyMap properties;
    ManifestContent content{};
    std::vector<ManifestEntry> dataFiles;
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
    static std::unique_ptr<Metadata> fromJson(std::string_view data, size_t capacity);

    Metadata() = default;

    std::string_view getUuid() const {
        return uuid;
    }

    std::string_view getLocation() const {
        return location;
    }

    Snapshot *findCurrentSnapshot();

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
