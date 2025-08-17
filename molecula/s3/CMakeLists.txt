add_library(
    molecula_s3
    STATIC

    S3Client.cpp
    S3ClientImpl.cpp
)

target_include_directories(
    molecula_s3
    PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    molecula_s3

    PUBLIC
    molecula_http_client
)
