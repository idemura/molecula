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

} // namespace molecula
