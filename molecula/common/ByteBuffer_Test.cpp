#include "molecula/common/ByteBuffer.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(ByteBuffer, Append) {
    ByteBuffer s;
    EXPECT_EQ(s.size(), 0);
    EXPECT_EQ(s.capacity(), 0);

    s.append("cat");
    EXPECT_EQ(s.size(), 3);
    EXPECT_EQ(s.capacity(), 64);
    EXPECT_EQ(s.view(), "cat");

    s.append("-&-lazy-dog");
    EXPECT_EQ(s.size(), 14);
    EXPECT_EQ(s.capacity(), 64);
    EXPECT_EQ(s.view(), "cat-&-lazy-dog");

    s.clear();
    EXPECT_EQ(s.size(), 0);
    EXPECT_EQ(s.view(), "");
}

GTEST_TEST(ByteBuffer, AppendBatches) {
    ByteBuffer s;
    for (int i = 0; i < 50; i++) {
        s.append("0123456789");
    }
    EXPECT_EQ(s.size(), 10 * 50);
    EXPECT_EQ(s.capacity(), 512);

    for (int i = 0; i < s.size(); i++) {
        EXPECT_EQ(s.data()[i], '0' + i % 10);
    }
}

} // namespace molecula
