#include <gtest/gtest.h>
#include <id_list.h>
#include "logger.h"

TEST(IdListTest, IdListIteratorTest) {
    id_list_t id_list(2);
    for(size_t i = 0; i < 10; i++) {
        id_list.upsert(i*2);
    }

    auto iter = id_list.new_iterator();

    for(size_t i = 0; i < 10; i++) {
        iter.skip_to(i*2);
        ASSERT_EQ(i*2, iter.id());
        ASSERT_TRUE(iter.valid());
    }

    iter.skip_to(19);
    ASSERT_FALSE(iter.valid());

    auto iter2 = id_list.new_iterator();
    size_t count = 0;
    while(iter2.valid()) {
        iter2.next();
        count++;
    }

    ASSERT_EQ(10, count);
    ASSERT_FALSE(iter2.valid());
}
