#include "molecula/common/Common.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(Common, String) {
  String s{"cat"};
  EXPECT_EQ(s.size(), 3);
  EXPECT_EQ(s.view(), "cat");
}

} // namespace molecula
