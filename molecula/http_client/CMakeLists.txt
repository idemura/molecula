add_library(molecula_http_client STATIC
    HttpClientCurl.cpp
)

target_include_directories(molecula_http_client
    PRIVATE
        ${CMAKE_SOURCE_DIR}
)

find_package(CURL REQUIRED)
target_link_libraries(molecula_http_client
    PRIVATE
        CURL::libcurl
        Folly::folly
        molecula_common
)
