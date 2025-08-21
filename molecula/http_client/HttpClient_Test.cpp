#include "molecula/http_client/HttpClient.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(HttpClient, lowerCaseHeader) {
    std::string header;

    header = "Hdr-Name: Value";
    lowerCaseHeader(header);
    EXPECT_EQ(header, "hdr-name: Value");

    header = "hdr-name: Value";
    lowerCaseHeader(header);
    EXPECT_EQ(header, "hdr-name: Value");

    header = "NoColon";
    lowerCaseHeader(header);
    EXPECT_EQ(header, "nocolon");
}

GTEST_TEST(HttpClient, HttpHeaders) {
    HttpHeaders headers;

    headers.add("Hdr-Name: Value.1");
    headers.add("Hdr: Value.2");
    headers.add("hdr-Plus: Value.3");

    EXPECT_EQ(headers.get("hdr-name"), "Value.1");
    EXPECT_EQ(headers.get("hdr"), "Value.2");
    EXPECT_EQ(headers.get("hdr-plus"), "Value.3");

    EXPECT_EQ(headers.get("Hdr-Name"), "");
    EXPECT_EQ(headers.get("Hdr"), "");
    EXPECT_EQ(headers.get("hdr-Plus"), "");
}

} // namespace molecula
