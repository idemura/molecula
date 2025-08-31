#include "molecula/iceberg/Iceberg.hpp"

#include "folly/Range.h"
#include "folly/compression/Compression.h"
#include "folly/compression/Zlib.h"
#include "folly/container/MapUtil.h"
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
void readProperties(
        json::dom::element element,
        std::unordered_map<std::string, std::string>& properties) {
    json::dom::object object;
    if (element.get(object) != json::SUCCESS) {
        throw std::runtime_error(kErrorMetadata);
    }
    for (const auto [key, e] : object) {
        std::string value;
        json::get_value(e, value);
        properties.emplace(std::string{key}, std::move(value));
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

private:
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

    Snapshot* snapshot{};
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

private:
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

    Metadata* metadata{};
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

enum class AvroType { Int32, Int64, String, Record };

std::unique_ptr<folly::compression::Codec> getAvroCompressionCodec(std::string_view name) {
    using namespace folly::compression;
    if (name == "deflate") {
        return zlib::getCodec(zlib::Options{zlib::Options::Format::RAW});
    }
    if (name == "null") {
        return getCodec(CodecType::NO_COMPRESSION);
    }
    LOG(ERROR) << "Unsupported Avro codec: " << name;
    return nullptr;
}

class ManifestListAvroSchemaField {
public:
    friend class ManifestListAvroSchemaFieldReader;

    int64_t id{};
    std::string name;
    std::string type;
};

class ManifestListAvroSchemaFieldReader {
public:
    explicit ManifestListAvroSchemaFieldReader(ManifestListAvroSchemaField* field) : field{field} {}

    void read(json::dom::element element) const {
        json::dom::object object;
        if (element.get(object) != json::SUCCESS) {
            throw std::runtime_error(kErrorManifestList);
        }
        for (const auto [key, element] : object) {
            lookupAndSet(key, element);
        }
    }

private:
    void lookupAndSet(std::string_view name, json::dom::element element) const {
        if (name == "field-id") {
            json::get_value(element, field->id);
        } else if (name == "name") {
            json::get_value(element, field->name);
        } else if (name == "type") {
            json::get_value(element, field->type);
        } else if (name == "doc") {
            // Do nothing
        }
    }

    ManifestListAvroSchemaField* field{};
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
                ManifestListAvroSchemaFieldReader{&field}.read(e);
                schema.push_back(std::move(field));
            }
        }
    }
}

std::unique_ptr<ManifestList> ManifestList::fromAvro(ByteBuffer& buffer) {
    AvroReader reader{buffer.data(), buffer.size()};
    reader.readMagic();

    // Read metadata
    std::unordered_map<std::string_view, std::string_view> metadata;
    auto metadataSize = reader.readInt();
    LOG(INFO) << "Metadata size: " << metadataSize;
    for (int64_t i = 0; i < metadataSize; i++) {
        // Do not inline in arguments - read order is important.
        auto key = reader.readString();
        auto value = reader.readString();
        LOG(INFO) << "Metadata: " << key << " = " << value;
        metadata.emplace(key, value);
    }
    // LOG(INFO)<<"end " <<reader.readInt();
    if (reader.readByte() != 0) {
        throw std::runtime_error(kErrorAvroRead);
    }

    auto sync1 = reader.readString(16);

    auto numObjects = reader.readInt();
    LOG(INFO) << "Number of objects: " << numObjects;
    auto data = reader.readString();
    LOG(INFO) << "Compressed size: " << data.size();
    LOG(INFO) << "Remaining size: " << reader.remaining();
    // LOG(INFO) << "Byte: " << (int)(unsigned char)reader.readByte();
    // if (reader.readByte() != 0) {
    //     LOG(ERROR) << "Invalid Avro file";
    //     return nullptr;
    // }
    auto sync2 = reader.readString(16);
    if (reader.remaining() != 0) {
        throw std::runtime_error(kErrorAvroRead);
    }

    auto codecName = folly::get_default(metadata, "avro.codec");
    std::unique_ptr<folly::compression::Codec> codec = getAvroCompressionCodec(codecName);
    if (!codec) {
        throw std::runtime_error(kErrorAvroCodec);
    }

    // Put it in its own buffer for simdjson.
    auto schemaJson = folly::get_default(metadata, "avro.schema");
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

    auto compressedBuf = folly::IOBuf::wrapBuffer(data.data(), data.size());
    std::unique_ptr<folly::IOBuf> uncompressedBuf = codec->uncompress(compressedBuf.get());
    uncompressedBuf->coalesce();

    AvroReader dataReader{(const char*)uncompressedBuf->data(), uncompressedBuf->length()};
    for (int64_t i = 0; i < numObjects; i++) {
        auto name = dataReader.readString();
        LOG(INFO) << "Manifest file: " << name;
    }

    auto manifestList = std::make_unique<ManifestList>();
    return manifestList;
}

} // namespace molecula::iceberg
