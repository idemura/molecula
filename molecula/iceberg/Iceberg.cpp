#include "molecula/iceberg/Iceberg.hpp"

#include <glog/logging.h>

namespace molecula {

// TODO: Store int64_t properties.
class PropertiesVisitor : public JsonVisitor {
public:
    JsonVisit visit(std::string_view name, std::string_view value) override {
        LOG(INFO) << "Property: " << name << " = " << value;
        properties->emplace(std::string{name}, std::string{value});
        return JsonVisit::Continue;
    }

    std::unordered_map<std::string, std::string>* properties{};
};

JsonVisit IcebergSnapshot::visit(std::string_view name, int64_t value) {
    if (name.size() < 2) {
        return JsonVisit::Continue;
    }
    switch (name[1]) {
        case 'c':
            if (name == "schema-id") {
                LOG(INFO) << "Schema id: " << value;
                schemaId = value;
            }
            return JsonVisit::Continue;
        case 'e':
            if (name == "sequence-number") {
                LOG(INFO) << "Sequence number: " << value;
                sequenceNumber = value;
            }
            return JsonVisit::Continue;
        case 'i':
            if (name == "timestamp-ms") {
                LOG(INFO) << "Timestamp: " << value;
                timestamp = std::chrono::milliseconds{value};
            }
            return JsonVisit::Continue;
        case 'n':
            if (name == "snapshot-id") {
                LOG(INFO) << "Snapshot id: " << value;
                id = value;
            }
            return JsonVisit::Continue;
    }
    return JsonVisit::Continue;
}

JsonVisit IcebergSnapshot::visit(std::string_view name, std::string_view value) {
    if (name == "manifest-list") {
        LOG(INFO) << "Manifest list: " << value;
        manifestList = value;
    }
    return JsonVisit::Continue;
    ;
}

template <typename T>
class ObjectArrayVisitor : public JsonVisitor {
public:
    // ObjectVisitor must implement the JsonVisitor interface.
    using ObjectVisitor = T;

    explicit ObjectArrayVisitor(std::vector<ObjectVisitor>* array) : array{array} {}

    JsonVisit visit(std::string_view name, JsonObject* object) override {
        ObjectVisitor visitor;
        if (jsonAccept(&visitor, object) == JsonVisit::Stop) {
            return JsonVisit::Stop;
        }
        array->emplace_back(std::move(visitor));
        return JsonVisit::Continue;
    }

private:
    std::vector<ObjectVisitor>* array{};
};

std::unique_ptr<IcebergMetadata> IcebergMetadata::fromJson(ByteBuffer& buffer) {
    auto metadata = std::make_unique<IcebergMetadata>();
    if (!jsonParse(buffer, metadata.get())) {
        return nullptr;
    }
    // TODO: Validate required fields.
    return metadata;
}

JsonVisit IcebergMetadata::visit(std::string_view name, int64_t value) {
    // Visitor allows us to make a very efficient dispatch.
    switch (name[0]) {
        case 'c':
            // Check for "current-xxx"
            if (name.size() < 10) {
                return JsonVisit::Continue;
            }
            switch (name[9]) {
                case 'c':
                    if (name == "current-schema-id") {
                        currentSchemaId = value;
                    }
                    return JsonVisit::Continue;
                case 'n':
                    if (name == "current-snapshot-id") {
                        currentSnapshotId = value;
                    }
                    return JsonVisit::Continue;
            }
            return JsonVisit::Continue;
        case 'l':
            if (name.size() < 6) {
                return JsonVisit::Continue;
            }
            switch (name[5]) {
                case 'c':
                    if (name == "last-column-id") {
                        lastColumnId = value;
                    }
                    return JsonVisit::Continue;
                case 's':
                    if (name == "last-sequence-number") {
                        lastSequenceNumber = value;
                    }
                    return JsonVisit::Continue;
                case 'u':
                    if (name == "last-updated-millis") {
                        lastUpdated = std::chrono::milliseconds{value};
                    }
                    return JsonVisit::Continue;
            }
            return JsonVisit::Continue;
    }
    return JsonVisit::Continue;
}

JsonVisit IcebergMetadata::visit(std::string_view name, std::string_view value) {
    // Visitor allows us to make a very efficient dispatch.
    switch (name[0]) {
        case 'l':
            if (name == "location") {
                location = value;
            }
            return JsonVisit::Continue;
        case 't':
            if (name == "table-uuid") {
                uuid = value;
            }
            return JsonVisit::Continue;
    }
    return JsonVisit::Continue;
}

JsonVisit IcebergMetadata::visit(std::string_view name, bool value) {
    return JsonVisit::Continue;
}

JsonVisit IcebergMetadata::visit(std::string_view name, JsonObject* node) {
    if (name == "properties") {
        PropertiesVisitor visitor;
        visitor.properties = &properties;
        return jsonAccept(&visitor, node);
    }
    return JsonVisit::Continue;
}

JsonVisit IcebergMetadata::visit(std::string_view name, JsonArray* node) {
    if (name == "snapshots") {
        ObjectArrayVisitor<IcebergSnapshot> visitor(&snapshots);
        return jsonAccept(&visitor, node);
    }
    return JsonVisit::Continue;
}

IcebergSnapshot* IcebergMetadata::findCurrentSnapshot() {
    for (auto& snapshot : snapshots) {
        if (snapshot.id == currentSnapshotId) {
            return &snapshot;
        }
    }
    return nullptr;
}

std::unique_ptr<IcebergManifestList> IcebergManifestList::fromAvro(ByteBuffer& buffer) {
    auto manifestList = std::make_unique<IcebergManifestList>();
    return manifestList;
}

} // namespace molecula
