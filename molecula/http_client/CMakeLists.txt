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
    molecula_http_client
    PRIVATE
    glog::glog
    CURL::libcurl
)

target_link_libraries(
    molecula_http_client
    PUBLIC
    Folly::folly
    molecula_common
)

if(MOLECULA_BUILD_TESTS)
    add_executable(
        molecula_http_client_test
        HttpClient_Test.cpp
    )

    target_link_libraries(
        molecula_http_client_test
        PRIVATE
        molecula_http_client
        GTest::gtest
        GTest::gtest_main
    )

    add_test(
        NAME molecula_http_client
        COMMAND
        molecula_http_client_test)
endif()
