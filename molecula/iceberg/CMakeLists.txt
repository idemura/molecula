file(COPY ${CMAKE_SOURCE_DIR}/molecula/iceberg/sql DESTINATION ${CMAKE_BINARY_DIR}/molecula/iceberg/sql)

add_library(
    molecula_iceberg
    STATIC
    Iceberg.cpp
    Iceberg.hpp
    IcebergMetadataDb.cpp
    IcebergMetadataDb.hpp
    json.cpp
    json.hpp
)

target_include_directories(
    molecula_iceberg
    PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/velox
)

target_link_libraries(
    molecula_iceberg
    PRIVATE
    glog::glog
    simdjson::simdjson
    SQLite::SQLite3
)

target_link_libraries(
    molecula_iceberg
    PUBLIC
    Folly::folly
    velox_encode
    molecula_common
)

if(MOLECULA_BUILD_TESTS)
    add_executable(
        molecula_iceberg_test
        Iceberg_Test.cpp
        IcebergMetadataDb_Test.cpp
    )

    target_link_libraries(
        molecula_iceberg_test
        PRIVATE
        molecula_iceberg
        GTest::gtest
        GTest::gtest_main
    )

    add_test(
        NAME molecula_iceberg
        COMMAND
        molecula_iceberg_test)
endif()
