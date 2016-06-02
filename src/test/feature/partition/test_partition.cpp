#include "gtest/gtest.h"

#include "lib/sql_util.h"


class TestPartition: public ::testing::Test
{
	public:
		TestPartition() {}
		~TestPartition() {}
};

TEST_F(TestPartition, TestPartitionNegativeAndBasics)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("partition/sql/partition_negetive_and_basics.sql",
                     "partition/ans/partition_negetive_and_basics.ans");
}

