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

GTEST_TEST(HttpClient, HttpBuffer) {
  HttpBuffer buffer;
  EXPECT_EQ(buffer.size(), 0);
  buffer.append("cat", 3);
  EXPECT_EQ(buffer.size(), 3);
  EXPECT_EQ(buffer.getStringView(), "cat");
  buffer.append("-&-dog", 6);
  EXPECT_EQ(buffer.size(), 9);
  EXPECT_EQ(buffer.getStringView(), "cat-&-dog");
}

} // namespace molecula
