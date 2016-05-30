#include "gtest/gtest.h"

#include "lib/sql-util.h"


class TestPartition: public ::testing::Test
{
	public:
		TestPartition() {}
		~TestPartition() {}
};

TEST_F(TestPartition, TestPartitionNegativeAndBasics)
{
	SQLUtility util;
	util.execSQLFile("partition/sql/partition_negetive_and_basics.sql",
                     "partition/ans/partition_negetive_and_basics.ans");
}

