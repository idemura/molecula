add_library(molecula_storage STATIC
    S3AsyncClient.cpp
)

target_include_directories(molecula_storage
    PRIVATE
        ${CMAKE_SOURCE_DIR}
)
