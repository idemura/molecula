add_library(molecula_storage STATIC
    S3AsyncClient.cpp
)

target_include_directories(molecula_storage
    PRIVATE
        ${CMAKE_SOURCE_DIR}
)

target_link_libraries(molecula_storage
    PRIVATE
        molecula_http_client
)
