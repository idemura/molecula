#include "molecula/iceberg/Iceberg.hpp"

#include "molecula/iceberg/json.hpp"

#include <glog/logging.h>

namespace molecula {

std::unique_ptr<IcebergMetadata> IcebergMetadata::fromJson(ByteBuffer& buffer) {
    auto metadata = std::make_unique<IcebergMetadata>();

    json::dom::element doc;

    json::dom::parser parser;
    bool reallocIfNeeded = buffer.capacity() - buffer.size() < json::SIMDJSON_PADDING;
    json::check(parser.parse(buffer.data(), buffer.size(), reallocIfNeeded).get(doc));

    json::get(doc["table-uuid"], metadata->uuid);
    json::get(doc["location"], metadata->location);
    json::get(doc["current-schema-id"], metadata->currentSchemaId);
    json::get(doc["current-snapshot-id"], metadata->currentSnapshotId);

    json::dom::array snapshots;
    json::check(doc["snapshots"].get(snapshots));
    for (json::dom::element snapshot : snapshots) {
        auto s = std::make_unique<IcebergSnapshot>();
        json::get(snapshot["snapshot-id"], s->id);
        json::get(snapshot["manifest-list"], s->manifestList);
        json::get(snapshot["schema-id"], s->schemaId);
        metadata->snapshots[s->id] = std::move(s);
    }

    json::dom::object properties;
    json::check(doc["properties"].get(properties));
    for (json::dom::key_value_pair kv : properties) {
        std::string_view s;
        json::check(kv.value.get(s));
        metadata->properties[std::string{kv.key}] = s;
        LOG(INFO) << "Loaded property: " << kv.key << " = " << s;
    }

    return std::move(metadata);
}

} // namespace molecula
