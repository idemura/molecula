add_library(
    molecula_s3
    STATIC
    S3Client.cpp
    S3Client.hpp
    S3ClientImpl.cpp
    S3ClientImpl.hpp
)

target_include_directories(
    molecula_s3
    PUBLIC
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    molecula_common
    PRIVATE
    glog::glog)

target_link_libraries(
    molecula_s3
    PUBLIC
    molecula_common
    molecula_http_client
)

if(MOLECULA_BUILD_TESTS)
    add_executable(
        molecula_s3_test
        S3Client_Test.cpp
    )

    target_link_libraries(
        molecula_s3_test
        PRIVATE
        molecula_s3
        GTest::gtest
        GTest::gtest_main
    )

    add_test(
        NAME molecula_s3
        COMMAND
        molecula_s3_test)
endif()
