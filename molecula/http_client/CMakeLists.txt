add_library(
    molecula_http_client
    STATIC
    HttpClient.cpp
    HttpClient.hpp
    HttpClientCurl.cpp
    HttpClientCurl.hpp
)

target_include_directories(
    molecula_http_client
    PUBLIC
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    molecula_common
    PRIVATE
    glog::glog)

target_link_libraries(
    molecula_http_client
    PRIVATE
    CURL::libcurl
)

target_link_libraries(
    molecula_http_client

    PUBLIC
    Folly::folly
    molecula_common
)
