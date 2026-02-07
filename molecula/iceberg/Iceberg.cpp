#include "molecula/iceberg/Iceberg.hpp"

#include "folly/Range.h"
#include "folly/String.h"
#include "folly/compression/Compression.h"
#include "folly/compression/Zlib.h"
#include "molecula/common/ByteBuffer.hpp"
#include "molecula/iceberg/json.hpp"
#include "velox/common/encode/Coding.h"

#include <glog/logging.h>

#include <stdexcept>

namespace velox = facebook::velox;

namespace molecula::iceberg {

const char *kErrorMetadata{"ICE00 Metadata"};
const char *kErrorManifestList{"ICE01 Manifest list"};
const char *kErrorManifest{"ICE02 Manifest"};
const char *kErrorJson{"ICE03 JSON"};
const char *kErrorAvro{"ICE03 Avro"};

// JSON properties reader
void readMetadataProperties(json::dom::element element, PropertyMap &properties) {
    json::dom::object object;
    if (element.get(object) != json::SUCCESS) {
        throw std::runtime_error(kErrorMetadata);
    }
    for (const auto [key, e] : object) {
        std::string_view value;
        json::get_value(e, value);
        properties.setProperty(key, value);
    }
}

class SnapshotReader {
public:
    explicit SnapshotReader(Snapshot *snapshot) : snapshot{snapshot} {}

    void read(json::dom::element element) const {
        json::dom::object object;
        if (element.get(object) != json::SUCCESS) {
            throw std::runtime_error(kErrorMetadata);
        }
        for (const auto [key, element] : object) {
            lookupAndSet(key, element);
        }
    }

    void lookupAndSet(std::string_view name, json::dom::element element) const {
        if (name.size() < 6) {
            return;
        }
        switch (name[1]) {
        case 'c':
            if (name == "schema-id") {
                json::get_value(element, snapshot->schemaId);
            }
            break;
        case 'e':
            if (name == "sequence-number") {
                json::get_value(element, snapshot->sequenceNumber);
            }
            break;
        case 'i':
            if (name == "timestamp-ms") {
                json::get_value(element, snapshot->timestamp);
            }
            break;
        case 'n':
            if (name == "snapshot-id") {
                json::get_value(element, snapshot->id);
            }
            break;
        case 'a':
            if (name == "manifest-list") {
                json::get_value(element, snapshot->manifestList);
            }
            break;
        }
    }

    Snapshot *const snapshot{};
};

class MetadataReader {
public:
    explicit MetadataReader(Metadata *metadata) : metadata{metadata} {}

    void read(std::string_view data, size_t capacity) const {
        json::dom::document doc;
        if (!json::parse(data, capacity, doc)) {
            throw std::runtime_error(kErrorMetadata);
        }
        json::dom::object object;
        if (doc.root().get(object) != json::SUCCESS) {
            throw std::runtime_error(kErrorMetadata);
        }
        for (const auto [key, element] : object) {
            lookupAndSet(key, element);
        }
    }

    void lookupAndSet(std::string_view name, json::dom::element element) const {
        switch (name[0]) {
        case 'c':
            // Check for "current-xxx"
            if (name.size() < 10) {
                return;
            }
            switch (name[9]) {
            case 'c':
                if (name == "current-schema-id") {
                    json::get_value(element, metadata->currentSchemaId);
                }
                break;
            case 'n':
                if (name == "current-snapshot-id") {
                    json::get_value(element, metadata->currentSnapshotId);
                }
                break;
            }
            break;
        case 'f':
            if (name == "format-version") {
                int64_t version{};
                json::get_value(element, version);
                // We only support format version 2.
                if (version != 2) {
                    LOG(ERROR) << "Unsupported Iceberg version: " << version;
                    throw std::runtime_error(kErrorMetadata);
                }
            }
            break;
        case 'l':
            if (name.size() < 6) {
                return;
            }
            switch (name[5]) {
            case 'c':
                if (name == "last-column-id") {
                    json::get_value(element, metadata->lastColumnId);
                }
                break;
            case 'i':
                if (name == "location") {
                    json::get_value(element, metadata->location);
                }
                break;
            case 's':
                if (name == "last-sequence-number") {
                    json::get_value(element, metadata->lastSequenceNumber);
                }
                break;
            case 'u':
                if (name == "last-updated-ms") {
                    json::get_value(element, metadata->lastUpdated);
                }
                break;
            }
            break;
        case 's':
            if (name == "snapshots") {
                json::dom::array array;
                if (element.get(array) != json::SUCCESS) {
                    throw std::runtime_error(kErrorMetadata);
                }
                for (json::dom::element e : array) {
                    Snapshot snapshot;
                    SnapshotReader{&snapshot}.read(e);
                    metadata->snapshots.push_back(std::move(snapshot));
                }
            }
            break;
        case 'p':
            if (name == "properties") {
                readMetadataProperties(element, metadata->properties);
            }
            break;
        case 't':
            if (name == "table-uuid") {
                json::get_value(element, metadata->uuid);
            }
            break;
        }
    }

    Metadata *const metadata{};
};

std::unique_ptr<Metadata> Metadata::fromJson(std::string_view data, size_t capacity) {
    auto metadata = std::make_unique<Metadata>();
    MetadataReader{metadata.get()}.read(data, capacity);
    return metadata;
}

Snapshot *Metadata::findCurrentSnapshot() {
    for (auto &snapshot : snapshots) {
        if (snapshot.id == currentSnapshotId) {
            return &snapshot;
        }
    }
    return nullptr;
}

// Avro data file reader. On error or end of data, methods throw.
// Class only contains methods to read data from Avro, but doesn't store it.
class AvroReader {
public:
    AvroReader(std::string_view data) : sp{data.data(), data.size()} {}

    size_t remaining() const {
        return sp.size();
    }

    void readMagic() {
        if (readString(4) != "Obj\x01") {
            throw std::runtime_error(kErrorAvro);
        }
    }

    char readByte() {
        if (sp.size() == 0) {
            throw std::runtime_error(kErrorAvro);
        }
        auto result = sp[0];
        sp.advance(1);
        return result;
    }

    void readByteAndCheck(int expected) {
        if (readByte() != expected) {
            throw std::runtime_error(kErrorAvro);
        }
    }

    int64_t readInt() {
        if (!velox::Varint::canDecode(sp)) {
            throw std::runtime_error(kErrorAvro);
        }
        auto p = sp.data();
        auto v = static_cast<int64_t>(velox::Varint::decode(&p, sp.size()));
        sp.advance(p - sp.data());
        return velox::ZigZag::decode(v);
    }

    std::string_view readString(size_t length) {
        if (sp.size() < length) {
            throw std::runtime_error(kErrorAvro);
        }
        std::string_view result{sp.data(), length};
        sp.advance(length);
        return result;
    }

    // Reads string length from data.
    std::string_view readString() {
        return readString(readInt());
    }

    void readMetadata(PropertyMap &properties) {
        auto size = readInt();
        for (int64_t i = 0; i < size; i++) {
            // Do not inline BOTH arguments: must read in key, value order.
            auto key = readString();
            properties.setProperty(key, readString());
        }
        readByteAndCheck(0);
    }

private:
    folly::StringPiece sp;
};

std::unique_ptr<folly::compression::Codec> getAvroCompressionCodec(std::string_view name) {
    using namespace folly::compression;
    if (name == "deflate") {
        return zlib::getCodec(zlib::Options{zlib::Options::Format::RAW});
    }
    if (name == "null") {
        return getCodec(CodecType::NO_COMPRESSION);
    }
    LOG(ERROR) << "Unsupported Avro codec: " << name;
    throw std::runtime_error(kErrorAvro);
}

std::unique_ptr<folly::IOBuf> decompressAvroData(std::string_view data, PropertyMap &properties) {
    auto codecName = properties.getProperty("avro.codec");
    auto codec = getAvroCompressionCodec(codecName);
    auto ioBuf = folly::IOBuf::wrapBuffer(data.data(), data.size());
    return codec->uncompress(ioBuf.get());
}

// Avro file parsed content.
class AvroContent {
public:
    // Avro metadata
    PropertyMap properties;

    int64_t numRecords{};

    // Uncompressed data
    std::unique_ptr<folly::IOBuf> data;

    explicit AvroContent(std::string_view buffer) {
        AvroReader reader{buffer};
        reader.readMagic();

        reader.readMetadata(properties);

        // Sync block (omit)
        (void)reader.readString(16);

        // Store number of records in the data block
        numRecords = reader.readInt();

        auto compressedData = reader.readString();
        data = decompressAvroData(compressedData, properties);
        data->coalesce();

        // Sync block (omit)
        (void)reader.readString(16);

        if (reader.remaining() != 0) {
            throw std::runtime_error(kErrorAvro);
        }
    }

    std::string_view getDataView() const {
        return std::string_view{(const char *)data->data(), data->length()};
    }
};

class ManifestListAvroSchemaField {
public:
    void read(json::dom::element element) {
        json::dom::object object;
        if (element.get(object) != json::SUCCESS) {
            throw std::runtime_error(kErrorManifestList);
        }
        for (const auto [key, element] : object) {
            lookupAndSet(key, element);
        }
    }

    void lookupAndSet(std::string_view name, json::dom::element element) {
        if (name == "field-id") {
            json::get_value(element, this->id);
        } else if (name == "name") {
            json::get_value(element, this->name);
        } else if (name == "type") {
            json::get_value(element, this->type);
        } else if (name == "doc") {
            // Do nothing
        } else if (name == "default") {
            this->optional = true;
        }
    }

    int64_t id{};
    std::string name;
    std::string type;
    bool optional{};
};

void readManifestListAvroSchema(
        json::dom::element element,
        std::vector<ManifestListAvroSchemaField> &schema) {
    json::dom::object object;
    if (element.get(object) != json::SUCCESS) {
        throw std::runtime_error(kErrorManifestList);
    }
    for (const auto [key, element] : object) {
        if (key == "type") {
            std::string_view type;
            json::get_value(element, type);
            if (type != "record") {
                throw std::runtime_error(kErrorManifestList);
            }
        } else if (key == "name") {
            std::string_view name;
            json::get_value(element, name);
            if (name != "manifest_file") {
                throw std::runtime_error(kErrorManifestList);
            }
        } else if (key == "fields") {
            json::dom::array array;
            if (element.get(array) != json::SUCCESS) {
                throw std::runtime_error(kErrorManifestList);
            }
            for (const auto e : array) {
                ManifestListAvroSchemaField field;
                field.read(e);
                schema.push_back(std::move(field));
            }
        }
    }
}

class ManifestListReader {
public:
    explicit ManifestListReader(ManifestList *manifestList) : manifestList{manifestList} {}

    void read(std::string_view data) const {
        AvroContent avro{data};

        // Put it in its own buffer for simdjson.
        auto schemaJson = avro.properties.getProperty("avro.schema");
        if (schemaJson.empty()) {
            throw std::runtime_error(kErrorAvro);
        }
        ByteBuffer schemaJsonBuffer{schemaJson.size() + 64};
        schemaJsonBuffer.append(schemaJson);
        json::dom::document avroSchemaDoc;
        if (!json::parse(schemaJsonBuffer.view(), schemaJsonBuffer.capacity(), avroSchemaDoc)) {
            throw std::runtime_error(kErrorAvro);
        }
        std::vector<ManifestListAvroSchemaField> schema;
        readManifestListAvroSchema(avroSchemaDoc.root(), schema);

        // Let's check the schema...

        AvroReader dataReader{avro.getDataView()};
        for (int64_t i = 0; i < avro.numRecords; i++) {
            ManifestListEntry entry;
            entry.manifestPath = dataReader.readString();
            entry.manifestLength = dataReader.readInt();
            LOG(INFO) << "partition_spec_id: " << dataReader.readInt();
            auto content = dataReader.readInt();
            switch (content) {
            case 0:
                entry.content = ManifestContent::Data;
                break;
            case 1:
                entry.content = ManifestContent::Deletes;
                break;
            default:
                LOG(ERROR) << "Unknown manifest content: " << content;
                throw std::runtime_error(kErrorManifestList);
            }
            entry.sequenceNumber = dataReader.readInt();
            LOG(INFO) << "min_sequence_number: " << dataReader.readInt();
            LOG(INFO) << "added_snapshot_id: " << dataReader.readInt();
            LOG(INFO) << "added_files_count: " << dataReader.readInt();
            LOG(INFO) << "existing_files_count: " << dataReader.readInt();
            LOG(INFO) << "deleted_files_count: " << dataReader.readInt();
            LOG(INFO) << "added_rows_count: " << dataReader.readInt();
            LOG(INFO) << "existing_rows_count: " << dataReader.readInt();
            LOG(INFO) << "deleted_rows_count: " << dataReader.readInt();
            LOG(INFO) << "partitions option: " << dataReader.readInt(); // 1, array
            LOG(INFO) << "partitions array size: "
                      << dataReader.readInt(); // 0 (size is null, array is over)
            LOG(INFO) << "key_metadata option: " << dataReader.readInt(); // 0, null
            manifestList->manifests.push_back(std::move(entry));
        }
        if (dataReader.remaining() != 0) {
            LOG(INFO) << "Remaining: " << dataReader.remaining();
            throw std::runtime_error(kErrorManifestList);
        }

        manifestList->properties = std::move(avro.properties);
    }

    ManifestList *const manifestList{};
};

std::unique_ptr<ManifestList> ManifestList::fromAvro(std::string_view data) {
    auto manifestList = std::make_unique<ManifestList>();
    ManifestListReader{manifestList.get()}.read(data);
    return manifestList;
}

class ManifestReader {
public:
    explicit ManifestReader(Manifest *manifest) : manifest{manifest} {}

    void read(std::string_view data) const {
        AvroContent avro{data};

        // Put it in its own buffer for simdjson.
        auto schemaJson = avro.properties.getProperty("avro.schema");
        LOG(INFO) << "Schema: " << schemaJson;
        if (schemaJson.empty()) {
            throw std::runtime_error(kErrorAvro);
        }
        ByteBuffer schemaJsonBuffer{schemaJson.size() + 64};
        schemaJsonBuffer.append(schemaJson);
        json::dom::document avroSchemaDoc;
        if (!json::parse(schemaJsonBuffer.view(), schemaJsonBuffer.capacity(), avroSchemaDoc)) {
            throw std::runtime_error(kErrorAvro);
        }
        // std::vector<ManifestListAvroSchemaField> schema;
        // readManifestListAvroSchema(avroSchemaDoc.root(), schema);

        // Let's check the schema...

        AvroReader dataReader{avro.getDataView()};
        for (int64_t i = 0; i < avro.numRecords; i++) {
            ManifestEntry entry;
            if (dataReader.readInt() == 2) {
                continue; // Deleted entry
            }
            if (dataReader.readInt() != 0) {
                LOG(INFO) << "snapshot_id " << dataReader.readInt();
            }
            entry.sequenceNumber = dataReader.readInt();
            entry.fileSequenceNumber = dataReader.readInt();
            switch (dataReader.readInt()) {
            case 0:
                entry.content = DataFileContent::Data;
                break;
            case 1:
                entry.content = DataFileContent::PositionDeletes;
                break;
            case 2:
                entry.content = DataFileContent::EqualityDeletes;
                break;
            default:
                throw std::runtime_error(kErrorManifest);
            }
            entry.filePath = dataReader.readString();
            entry.fileFormat = dataReader.readString();
            entry.recordCount = dataReader.readInt();
            entry.fileSize = dataReader.readInt();
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "column_sizes:";
                int64_t arraySize = dataReader.readInt();
                for (int64_t j = 0; j < arraySize; j++) {
                    LOG(INFO) << "key " << dataReader.readInt();
                    LOG(INFO) << "value " << dataReader.readInt();
                }
                LOG(INFO) << "column_sizes trailer " << dataReader.readInt();
            } else {
                LOG(INFO) << "column_sizes is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "value_counts:";
                int64_t arraySize = dataReader.readInt();
                for (int64_t j = 0; j < arraySize; j++) {
                    LOG(INFO) << "key " << dataReader.readInt();
                    LOG(INFO) << "value " << dataReader.readInt();
                }
                LOG(INFO) << "value_counts trailer " << dataReader.readInt();
            } else {
                LOG(INFO) << "value_counts is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "null_value_counts:";
                int64_t arraySize = dataReader.readInt();
                for (int64_t j = 0; j < arraySize; j++) {
                    LOG(INFO) << "key " << dataReader.readInt();
                    LOG(INFO) << "value " << dataReader.readInt();
                }
                LOG(INFO) << "null_value_counts trailer " << dataReader.readInt();
            } else {
                LOG(INFO) << "null_value_counts is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "nan_value_counts:";
                int64_t arraySize = dataReader.readInt();
                while (arraySize > 0) {
                    for (int64_t j = 0; j < arraySize; j++) {
                        LOG(INFO) << "key " << dataReader.readInt();
                        LOG(INFO) << "value " << dataReader.readInt();
                    }
                    arraySize = dataReader.readInt();
                }
            } else {
                LOG(INFO) << "nan_value_counts is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "lower_bounds:";
                int64_t arraySize = dataReader.readInt();
                for (int64_t j = 0; j < arraySize; j++) {
                    LOG(INFO) << "key " << dataReader.readInt();
                    LOG(INFO) << "value " << folly::hexlify(dataReader.readString());
                }
                LOG(INFO) << "lower_bounds trailer " << dataReader.readInt();
            } else {
                LOG(INFO) << "lower_bounds is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "upper_bounds:";
                int64_t arraySize = dataReader.readInt();
                for (int64_t j = 0; j < arraySize; j++) {
                    LOG(INFO) << "key " << dataReader.readInt();
                    LOG(INFO) << "value " << folly::hexlify(dataReader.readString());
                }
                LOG(INFO) << "upper_bounds trailer " << dataReader.readInt();
            } else {
                LOG(INFO) << "upper_bounds is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "key_metadata: " << folly::hexlify(dataReader.readString());
            } else {
                LOG(INFO) << "key_metadata is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "split_offsets:";
                int64_t arraySize = dataReader.readInt();
                for (int64_t j = 0; j < arraySize; j++) {
                    LOG(INFO) << "offset " << dataReader.readInt();
                }
                LOG(INFO) << "split_offsets trailer " << dataReader.readInt();
            } else {
                LOG(INFO) << "split_offsets is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "equality_ids:";
                int64_t arraySize = dataReader.readInt();
                for (int64_t j = 0; j < arraySize; j++) {
                    LOG(INFO) << "offset " << dataReader.readInt();
                }
                LOG(INFO) << "equality_ids trailer " << dataReader.readInt();
            } else {
                LOG(INFO) << "equality_ids is null";
            }
            if (dataReader.readInt() == 1) {
                LOG(INFO) << "sort_order_id: " << dataReader.readInt();
            } else {
                LOG(INFO) << "sort_order_id is null";
            }
            // entry.manifestPath = dataReader.readString();
            manifest->dataFiles.push_back(std::move(entry));
        }
        if (dataReader.remaining() != 0) {
            LOG(ERROR) << "Remaining: " << dataReader.remaining();
            throw std::runtime_error(kErrorManifest);
        }

        manifest->properties = std::move(avro.properties);
    }

    void readContentHeader(PropertyMap &properties) const {
        auto content = properties.getProperty("content");
        if (content == "data") {
            manifest->content = ManifestContent::Data;
        } else if (content == "deletes") {
            manifest->content = ManifestContent::Deletes;
        } else {
            throw std::runtime_error(kErrorManifest);
        }
    }

    Manifest *const manifest{};
};

std::unique_ptr<Manifest> Manifest::fromAvro(std::string_view data) {
    auto manifest = std::make_unique<Manifest>();
    ManifestReader{manifest.get()}.read(data);
    return manifest;
}

} // namespace molecula::iceberg
