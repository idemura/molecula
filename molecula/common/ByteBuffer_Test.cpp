#include "molecula/common/ByteBuffer.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(Common, ByteBuffer) {
  ByteBuffer s;
  EXPECT_EQ(s.size(), 0);

  s.append("cat");
  EXPECT_EQ(s.size(), 3);
  EXPECT_EQ(s.view(), "cat");

  s.append("-&-lazy-dog");
  EXPECT_EQ(s.size(), 14);
  EXPECT_EQ(s.view(), "cat-&-lazy-dog");

  s.clear();
  EXPECT_EQ(s.size(), 0);
  EXPECT_EQ(s.view(), "");
}

} // namespace molecula
