#include "molecula/iceberg/Iceberg.hpp"

#include "folly/Range.h"
#include "folly/compression/Compression.h"
#include "folly/compression/Zlib.h"
#include "molecula/iceberg/json.hpp"
#include "velox/common/encode/Coding.h"

#include <glog/logging.h>

#include <stdexcept>

namespace velox = facebook::velox;

namespace molecula::iceberg {

const char* kErrorMetadata{"ICE-00 Metadata JSON"};
const char* kErrorMetadataVersion{"ICE-01 Metadata version"};
const char* kErrorAvroRead{"ICE-01 Avro read"};
const char* kErrorAvroCodec{"ICE-02 Avro codec"};
const char* kErrorAvroSchema{"ICE-03 Avro schema"};
const char* kErrorManifestList{"ICE-04 Manifest list"};

// JSON properties reader
void readProperties(json::dom::element element, PropertyMap& properties) {
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
    explicit SnapshotReader(Snapshot* snapshot) : snapshot{snapshot} {}

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

    Snapshot* const snapshot{};
};

class MetadataReader {
public:
    explicit MetadataReader(Metadata* metadata) : metadata{metadata} {}

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
                    throw std::runtime_error(kErrorMetadataVersion);
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
                readProperties(element, metadata->properties);
            }
            break;
        case 't':
            if (name == "table-uuid") {
                json::get_value(element, metadata->uuid);
            }
            break;
        }
    }

    Metadata* const metadata{};
};

std::unique_ptr<Metadata> Metadata::fromJson(ByteBuffer& buffer) {
    json::dom::document doc;
    if (!json::parse(buffer, doc)) {
        throw std::runtime_error(kErrorMetadata);
    }
    auto metadata = std::make_unique<Metadata>();
    MetadataReader{metadata.get()}.read(doc.root());
    return metadata;
}

Snapshot* Metadata::findCurrentSnapshot() {
    for (auto& snapshot : snapshots) {
        if (snapshot.id == currentSnapshotId) {
            return &snapshot;
        }
    }
    return nullptr;
}

// Data reader from Avro file. If reached end of data, all read operations will return
// default values.
class AvroReader {
public:
    AvroReader(const char* data, size_t size) : sp{data, size} {}

    size_t remaining() const {
        return sp.size();
    }

    void readMagic() {
        if (readString(4) != "Obj\x01") {
            throw std::runtime_error(kErrorAvroRead);
        }
    }

    char readByte() {
        if (sp.size() == 0) {
            throw std::runtime_error(kErrorAvroRead);
        }
        auto result = sp[0];
        sp.advance(1);
        return result;
    }

    void readByteAndCheck0() {
        if (readByte() != 0) {
            throw std::runtime_error(kErrorAvroRead);
        }
    }

    int64_t readInt() {
        if (!velox::Varint::canDecode(sp)) {
            throw std::runtime_error(kErrorAvroRead);
        }
        auto p = sp.data();
        auto v = static_cast<int64_t>(velox::Varint::decode(&p, sp.size()));
        sp.advance(p - sp.data());
        return velox::ZigZag::decode(v);
    }

    std::string_view readString(size_t length) {
        if (sp.size() < length) {
            throw std::runtime_error(kErrorAvroRead);
        }
        std::string_view result{sp.data(), length};
        sp.advance(length);
        return result;
    }

    // Reads string length from data.
    std::string_view readString() {
        return readString(readInt());
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
    throw std::runtime_error(kErrorAvroCodec);
}

void readAvroMetadata(AvroReader& reader, PropertyMap& properties) {
    auto metadataSize = reader.readInt();
    for (int64_t i = 0; i < metadataSize; i++) {
        // Do not inline BOTH arguments: must read in key, value order.
        auto key = reader.readString();
        properties.setProperty(key, reader.readString());
    }
    reader.readByteAndCheck0();
}

std::unique_ptr<folly::IOBuf> decompressAvroData(std::string_view data, PropertyMap& properties) {
    auto codecName = properties.getProperty("avro.codec");
    auto codec = getAvroCompressionCodec(codecName);

    std::unique_ptr<folly::IOBuf> buf = folly::IOBuf::wrapBuffer(data.data(), data.size());
    std::unique_ptr<folly::IOBuf> uncompressedBuf = codec->uncompress(buf.get());
    uncompressedBuf->coalesce();
    return uncompressedBuf;
}

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
        std::vector<ManifestListAvroSchemaField>& schema) {
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
    explicit ManifestListReader(ManifestList* manifestList) : manifestList{manifestList} {}

    void read(AvroReader reader) const {
        reader.readMagic();

        readAvroMetadata(reader, manifestList->properties);

        auto sync1 = reader.readString(16);

        auto numObjects = reader.readInt();
        LOG(INFO) << "Number of objects: " << numObjects;
        auto compressedData = reader.readString();
        LOG(INFO) << "Compressed size: " << compressedData.size();

        auto sync2 = reader.readString(16);
        if (reader.remaining() != 0) {
            throw std::runtime_error(kErrorAvroRead);
        }

        // Put it in its own buffer for simdjson.
        auto schemaJson = manifestList->properties.getProperty("avro.schema");
        if (schemaJson.empty()) {
            throw std::runtime_error(kErrorAvroSchema);
        }
        ByteBuffer schemaJsonBuffer{schemaJson.size() + 64};
        schemaJsonBuffer.append(schemaJson);
        json::dom::document avroSchemaDoc;
        if (!json::parse(schemaJsonBuffer, avroSchemaDoc)) {
            throw std::runtime_error(kErrorAvroSchema);
        }
        std::vector<ManifestListAvroSchemaField> schema;
        readManifestListAvroSchema(avroSchemaDoc.root(), schema);

        // Let's check the schema...

        std::unique_ptr<folly::IOBuf> data =
                decompressAvroData(compressedData, manifestList->properties);
        AvroReader dataReader{(const char*)data->data(), data->length()};

        for (int64_t i = 0; i < numObjects; i++) {
            LOG(INFO) << "manifest_path: " << dataReader.readString();
            LOG(INFO) << "manifest_length: " << dataReader.readInt();
            LOG(INFO) << "partition_spec_id: " << dataReader.readInt();
            LOG(INFO) << "content (0, 1): " << dataReader.readInt();
            LOG(INFO) << "sequence_number: " << dataReader.readInt();
            LOG(INFO) << "min_sequence_number: " << dataReader.readInt();
            LOG(INFO) << "added_snapshot_id: " << dataReader.readInt();
            LOG(INFO) << "added_files_count: " << dataReader.readInt();
            LOG(INFO) << "existing_files_count: " << dataReader.readInt();
            LOG(INFO) << "deleted_files_count: " << dataReader.readInt();
            LOG(INFO) << "added_rows_count: " << dataReader.readInt();
            LOG(INFO) << "existing_rows_count: " << dataReader.readInt();
            LOG(INFO) << "deleted_rows_count: " << dataReader.readInt();
            LOG(INFO) << "opt1: " << dataReader.readInt(); // 1
            LOG(INFO) << "opt2: " << dataReader.readInt(); // 0
            LOG(INFO) << "opt3: " << dataReader.readInt(); // 0
            // LOG(INFO) << "opt2: " << (int) dataReader.readByte();
            LOG(INFO) << "remaining: " << dataReader.remaining();
        }
    }

    ManifestList* const manifestList{};
};

std::unique_ptr<ManifestList> ManifestList::fromAvro(ByteBuffer& buffer) {
    auto manifestList = std::make_unique<ManifestList>();
    ManifestListReader{manifestList.get()}.read(AvroReader{buffer.data(), buffer.size()});
    return manifestList;
}

} // namespace molecula::iceberg
