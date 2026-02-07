#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <sqlite3.h>

namespace molecula::iceberg {

class MetadataDbConfig {
public:
    std::string_view dbFile;
    std::filesystem::path sqlTemplatesPath;
    std::string_view schema;
};

class MetadataDb {
public:
    explicit MetadataDb(const MetadataDbConfig &config);
    ~MetadataDb() {
        close();
    }

    void open();
    void close();
    void createTables();

private:
    std::string dbFile;
    std::string sqlTemplatesPath;
    std::string schema;

    sqlite3 *db = nullptr;
};

} // namespace molecula::iceberg
