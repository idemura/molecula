#include "molecula/common/PropertyMap.hpp"

#include <gtest/gtest.h>

namespace molecula {

GTEST_TEST(PropertyMap, GetSet) {
    PropertyMap props;
    props.setProperty("key1", "value1");
    props.setProperty("key2", "value2");

    EXPECT_EQ(props.getProperty("key1"), "value1");
    EXPECT_EQ(props.getProperty("key2"), "value2");
    EXPECT_EQ(props.getProperty("key3"), "");
}

} // namespace molecula
