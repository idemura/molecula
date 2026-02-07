#include "molecula/iceberg/IcebergMetadataDb.hpp"

#include <glog/logging.h>

#include <stdexcept>

namespace molecula::iceberg {

MetadataDb::MetadataDb(const MetadataDbConfig &config) :
    dbFile{config.dbFile}, sqlTemplatesPath{config.sqlTemplatesPath}, schema{config.schema} {}

void MetadataDb::open() {
    if (db) {
        throw std::runtime_error("Metadata DB is already open");
    }
    if (sqlite3_open(dbFile.c_str(), &db) != SQLITE_OK) {
        throw std::runtime_error("Failed to open metadata DB");
    }
    LOG(INFO) << "SQL templates path: " << sqlTemplatesPath;
    for (const auto &entry : std::filesystem::directory_iterator(sqlTemplatesPath)) {
        if (entry.is_regular_file()) {
            LOG(INFO) << entry.path().filename() << "\n";
        }
    }
}

void MetadataDb::close() {
    if (db) {
        sqlite3_close(db);
        db = nullptr;
    }
}

void MetadataDb::createTables() {
    const char *sql = R"(
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL
        );
    )";
    char *errMsg = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        LOG(INFO) << "Failed to create table: " << (errMsg ? errMsg : "Unknown error");
        sqlite3_free(errMsg);
        throw std::runtime_error("Failed to create table");
    }
}

} // namespace molecula::iceberg
