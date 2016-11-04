#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestAOSnappy: public ::testing::Test
{
	public:
		const string initFile = "ao/sql/init_file";
		TestAOSnappy() { }
		~TestAOSnappy() {}
};

TEST_F(TestAOSnappy, Create1048576)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_crtb_with_row_snappy_1048576.sql",
	                 "ao/ans/ao_crtb_with_row_snappy_1048576.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Create2097152)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_crtb_with_row_snappy_2097152.sql",
                     "ao/ans/ao_crtb_with_row_snappy_2097152.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Create32768)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_crtb_with_row_snappy_32768.sql",
	                 "ao/ans/ao_crtb_with_row_snappy_32768.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Create65536)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_crtb_with_row_snappy_65536.sql",
	                 "ao/ans/ao_crtb_with_row_snappy_65536.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Create8192)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_crtb_with_row_snappy_8192.sql",
	                 "ao/ans/ao_crtb_with_row_snappy_8192.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Partition1048576)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_partsnappy1048576.sql",
	                 "ao/ans/ao_wt_partsnappy1048576.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Partition2097152)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_partsnappy2097152.sql",
	                 "ao/ans/ao_wt_partsnappy2097152.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Partition32768)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_partsnappy32768.sql",
	                 "ao/ans/ao_wt_partsnappy32768.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Partition65536)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_partsnappy65536.sql",
	                 "ao/ans/ao_wt_partsnappy65536.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, Partition8192)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_partsnappy8192.sql",
	                 "ao/ans/ao_wt_partsnappy8192.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, SubPartition1048576)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_sub_partsnappy1048576.sql",
	                 "ao/ans/ao_wt_sub_partsnappy1048576.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, SubPartition32768)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_sub_partsnappy32768.sql",
	                 "ao/ans/ao_wt_sub_partsnappy32768.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, SubPartition65536)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_sub_partsnappy65536.sql",
	                 "ao/ans/ao_wt_sub_partsnappy65536.ans",
	                 initFile);
}

TEST_F(TestAOSnappy, SubPartition8192)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("ao/sql/ao_wt_sub_partsnappy8192.sql",
	                 "ao/ans/ao_wt_sub_partsnappy8192.ans",
	                 initFile);
}

