#include "molecula/iceberg/Iceberg.hpp"

#include "folly/compression/Compression.h"
#include "folly/compression/Zlib.h"
#include "folly/container/MapUtil.h"
#include "velox/common/encode/Coding.h"

#include <glog/logging.h>

namespace velox = facebook::velox;

namespace molecula::iceberg {

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

JsonVisit Snapshot::visit(std::string_view name, int64_t value) {
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

JsonVisit Snapshot::visit(std::string_view name, std::string_view value) {
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

std::unique_ptr<Metadata> Metadata::fromJson(ByteBuffer& buffer) {
    auto metadata = std::make_unique<Metadata>();
    if (!jsonParse(buffer, metadata.get())) {
        return nullptr;
    }
    // TODO: Validate required fields.
    return metadata;
}

JsonVisit Metadata::visit(std::string_view name, int64_t value) {
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
        case 'f':
            if (name == "format-version") {
                // We only support format version 2.
                if (value != 2) {
                    LOG(ERROR) << "Unsupported format version: " << value;
                    return JsonVisit::Stop;
                }
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

JsonVisit Metadata::visit(std::string_view name, std::string_view value) {
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

JsonVisit Metadata::visit(std::string_view name, bool value) {
    return JsonVisit::Continue;
}

JsonVisit Metadata::visit(std::string_view name, JsonObject* node) {
    if (name == "properties") {
        PropertiesVisitor visitor;
        visitor.properties = &properties;
        return jsonAccept(&visitor, node);
    }
    return JsonVisit::Continue;
}

JsonVisit Metadata::visit(std::string_view name, JsonArray* node) {
    if (name == "snapshots") {
        ObjectArrayVisitor<Snapshot> visitor(&snapshots);
        return jsonAccept(&visitor, node);
    }
    return JsonVisit::Continue;
}

Snapshot* Metadata::findCurrentSnapshot() {
    for (auto& snapshot : snapshots) {
        if (snapshot.id == currentSnapshotId) {
            return &snapshot;
        }
    }
    return nullptr;
}

char AvroReader::readByte() {
    if (sp.size() == 0) {
        return 0;
    }
    auto result = sp[0];
    sp.advance(1);
    return result;
}

int64_t AvroReader::readInt() {
    if (!velox::Varint::canDecode(sp)) {
        sp.advance(sp.size());
        return 0;
    }
    auto p = sp.data();
    auto v = static_cast<int64_t>(velox::Varint::decode(&p, sp.size()));
    sp.advance(p - sp.data());
    return velox::ZigZag::decode(v);
}

std::string_view AvroReader::readString(size_t length) {
    if (sp.size() < length) {
        // Read to the end
        sp.advance(sp.size());
        return std::string_view{};
    }
    std::string_view result{sp.data(), length};
    sp.advance(length);
    return result;
}

std::string_view AvroReader::readString() {
    return readString(readInt());
}

std::unique_ptr<folly::compression::Codec> getAvroCompressionCodec(std::string_view name) {
    using namespace folly::compression;
    if (name == "deflate") {
        return zlib::getCodec(zlib::Options{zlib::Options::Format::RAW});
    } else if (name == "null") {
        return getCodec(CodecType::NO_COMPRESSION);
    } else {
        LOG(ERROR) << "Unsupported Avro codec: " << name;
        return nullptr;
    }
}

std::unique_ptr<ManifestList> ManifestList::fromAvro(ByteBuffer& buffer) {
    AvroReader reader{buffer.data(), buffer.size()};
    if (reader.readString(4) != "Obj\x01") {
        LOG(ERROR) << "Invalid Avro data";
        return nullptr;
    }

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
        LOG(ERROR) << "Invalid Avro data";
        return nullptr;
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

    auto codecName = folly::get_default(metadata, "avro.codec");
    std::unique_ptr<folly::compression::Codec> codec = getAvroCompressionCodec(codecName);
    if (!codec) {
        return nullptr;
    }

    auto compressedBuf = folly::IOBuf::wrapBuffer(data.data(), data.size());
    // folly::Optional<uint64_t> uncompressedLength = codec->getUncompressedLength(
    //         compressedBuf.get(), folly::Optional<uint64_t>());
    // if (uncompressedLength) {
    //     LOG(INFO) << "Uncompressed length: " << *uncompressedLength;
    // }
    LOG(INFO) << "Before uncompress";
    std::unique_ptr<folly::IOBuf> uncompressedData = codec->uncompress(compressedBuf.get());
    uncompressedData->coalesce();

    AvroReader dataReader{(const char*)uncompressedData->data(), uncompressedData->length()};
    for (int64_t i = 0; i < numObjects; i++) {
        auto name = dataReader.readString();
        LOG(INFO) << "Manifest file: " << name;
    }

    auto manifestList = std::make_unique<ManifestList>();
    return manifestList;
}

} // namespace molecula::iceberg
