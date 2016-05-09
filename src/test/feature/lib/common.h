#ifndef FILE_58f9fa9e_188f_5f43_d103_c16c3132fd24_HPP
#define FILE_58f9fa9e_188f_5f43_d103_c16c3132fd24_HPP

#include <vector>
#include <gtest/gtest.h>

namespace hawq {
namespace test {

template <class V>
void EQUAL_CHECK(std::vector<V> a,
                 std::vector<V> b) {
  EXPECT_EQ(a.size(), b.size());
  for(size_t i = 0; i < a.size(); ++i) {
    EXPECT_EQ(a[i], b[i]);
  }
}

} // namespace test
} // namespace hawq

#endif
