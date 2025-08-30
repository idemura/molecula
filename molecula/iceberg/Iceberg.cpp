#include "molecula/iceberg/Iceberg.hpp"

#include <glog/logging.h>

namespace molecula {

// TODO: Store int64_t properties.
class PropertiesVisitor : public JsonVisitor {
public:
    bool visit(std::string_view name, std::string_view value) override {
        LOG(INFO) << "Property: " << name << " = " << value;
        properties->emplace(std::string{name}, std::string{value});
        return true;
    }

    std::unordered_map<std::string, std::string>* properties{};
};

bool IcebergSnapshot::visit(std::string_view name, int64_t value) {
    if (name.size() < 2) {
        return true;
    }
    switch (name[1]) {
        case 'c':
            if (name == "schema-id") {
                LOG(INFO) << "Schema id: " << value;
                schemaId = value;
            }
            break;
        case 'e':
            if (name == "sequence-number") {
                LOG(INFO) << "Sequence number: " << value;
                sequenceNumber = value;
            }
            break;
        case 'i':
            if (name == "timestamp-ms") {
                LOG(INFO) << "Timestamp: " << value;
                timestamp = std::chrono::milliseconds{value};
            }
            break;
        case 'n':
            if (name == "snapshot-id") {
                LOG(INFO) << "Snapshot id: " << value;
                id = value;
            }
            break;
    }
    return true;
}

bool IcebergSnapshot::visit(std::string_view name, std::string_view value) {
    if (name == "manifest-list") {
        LOG(INFO) << "Manifest list: " << value;
        manifestList = value;
    }
    return true;
}

template <typename T>
class ObjectArrayVisitor : public JsonVisitor {
public:
    // ObjectVisitor must implement the JsonVisitor interface.
    using ObjectVisitor = T;

    explicit ObjectArrayVisitor(std::vector<ObjectVisitor>* array) : array{array} {}

    bool visit(std::string_view name, JsonObject* object) override {
        ObjectVisitor visitor;
        if (!jsonAccept(&visitor, object)) {
            return false;
        }
        array->emplace_back(std::move(visitor));
        return true;
    }

private:
    std::vector<ObjectVisitor>* array{};
};

std::unique_ptr<IcebergMetadata> IcebergMetadata::fromJson(ByteBuffer& buffer) {
    auto metadata = std::make_unique<IcebergMetadata>();
    if (!jsonParse(buffer, metadata.get())) {
        return nullptr;
    }
    return metadata;
}

bool IcebergMetadata::visit(std::string_view name, int64_t value) {
    // Visitor allows us to make a very efficient dispatch.
    switch (name[0]) {
        case 'c':
            // Check for "current-xxx"
            if (name.size() < 10) {
                return true;
            }
            switch (name[9]) {
                case 'c':
                    if (name == "current-schema-id") {
                        currentSchemaId = value;
                    }
                    return true;
                case 'n':
                    if (name == "current-snapshot-id") {
                        currentSnapshotId = value;
                    }
                    return true;
            }
            return true;
        case 'l':
            if (name.size() < 6) {
                return true;
            }
            switch (name[5]) {
                case 'c':
                    if (name == "last-column-id") {
                        lastColumnId = value;
                    }
                    return true;
                case 's':
                    if (name == "last-sequence-number") {
                        lastSequenceNumber = value;
                    }
                    return true;
                case 'u':
                    if (name == "last-updated-millis") {
                        lastUpdated = std::chrono::milliseconds{value};
                    }
                    return true;
            }
            return true;
    }
    return true;
}

bool IcebergMetadata::visit(std::string_view name, std::string_view value) {
    // Visitor allows us to make a very efficient dispatch.
    switch (name[0]) {
        case 'l':
            if (name == "location") {
                location = value;
            }
            return true;
        case 't':
            if (name == "table-uuid") {
                uuid = value;
            }
            return true;
    }
    return true;
}

bool IcebergMetadata::visit(std::string_view name, bool value) {
    return true;
}

bool IcebergMetadata::visit(std::string_view name, JsonObject* node) {
    if (name == "properties") {
        PropertiesVisitor visitor;
        visitor.properties = &properties;
        return jsonAccept(&visitor, node);
    }
    return true;
}

bool IcebergMetadata::visit(std::string_view name, JsonArray* node) {
    if (name == "snapshots") {
        ObjectArrayVisitor<IcebergSnapshot> visitor(&snapshots);
        return jsonAccept(&visitor, node);
    }
    return true;
}

} // namespace molecula
