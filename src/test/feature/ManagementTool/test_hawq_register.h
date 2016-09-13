#ifndef TEST_HAWQ_REGISTER_H
#define TEST_HAWQ_REGISTER_H

#include <string>
#include "lib/hdfs_config.h"
#include "gtest/gtest.h"

class TestHawqRegister : public ::testing::Test {
 public:
  TestHawqRegister() {}
  ~TestHawqRegister() {}
  std::string getHdfsLocation() {
    hawq::test::HdfsConfig hc;
    std::string namenodehost = "";
    EXPECT_EQ(true, hc.getNamenodeHost(namenodehost));
    return hawq::test::stringFormat("hdfs://%s", namenodehost.c_str());
  }
};

#endif
