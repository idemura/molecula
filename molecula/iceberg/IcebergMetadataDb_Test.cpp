#include "molecula/iceberg/IcebergMetadataDb.hpp"

#include <gtest/gtest.h>
#include <cstdlib>

namespace molecula::iceberg {

std::filesystem::path getBasePath() {
    auto env = std::getenv("TEST_DIR");
    return env ? std::filesystem::path{env} : std::filesystem::current_path();
}

GTEST_TEST(IcebergMetadataDb, CreateTables) {
    MetadataDbConfig config;
    config.dbFile = ":memory:";
    config.sqlTemplatesPath = getBasePath() / "sql";
    config.schema = "test_schema";

    MetadataDb db{config};
    db.open();
    db.createTables();
}

} // namespace molecula::iceberg
