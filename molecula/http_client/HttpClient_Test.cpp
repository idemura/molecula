#include "molecula/http_client/HttpClient.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(HttpClient, lowerCaseHeader) {
  std::string header;
  header = "Hdr-Name: Value";
  EXPECT_EQ(lowerCaseHeader(header), "hdr-name: Value");
  header = "hdr-name: Value";
  EXPECT_EQ(lowerCaseHeader(header), "hdr-name: Value");
  header = "NoColon";
  EXPECT_EQ(lowerCaseHeader(header), "nocolon");
}

GTEST_TEST(HttpClient, HttpResponse) {
  HttpResponse response;
  EXPECT_EQ(response.getStatus(), 0);
  response.setStatus(200);
  EXPECT_EQ(response.getStatus(), 200);

  response.addHeader("Hdr-Name: Value.1");
  response.addHeader("Hdr: Value.2");
  response.addHeader("hdr-Plus: Value.3");

  EXPECT_EQ(response.getHeaderValue("hdr-name"), "Value.1");
  EXPECT_EQ(response.getHeaderValue("hdr"), "Value.2");
  EXPECT_EQ(response.getHeaderValue("hdr-plus"), "Value.3");

  EXPECT_EQ(response.getHeaderValue("Hdr-Name"), "");
  EXPECT_EQ(response.getHeaderValue("Hdr"), "");
  EXPECT_EQ(response.getHeaderValue("hdr-Plus"), "");
}

} // namespace molecula
