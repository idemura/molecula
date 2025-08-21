add_library(
    molecula_iceberg
    STATIC
    Iceberg.cpp
    Iceberg.hpp
)

target_include_directories(
    molecula_iceberg
    PUBLIC
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    molecula_iceberg
    PRIVATE
    glog::glog
    simdjson::simdjson
)

target_link_libraries(
    molecula_iceberg
    PUBLIC
    Folly::folly
    molecula_common
)

if(MOLECULA_BUILD_TESTS)
    add_executable(
        molecula_iceberg_test
        Iceberg_Test.cpp
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
