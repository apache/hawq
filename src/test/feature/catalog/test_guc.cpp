#include "gtest/gtest.h"

#include "lib/sql_util.h"
#include "lib/file_replace.h"

using std::string;
using hawq::test::FileReplace;

class TestGuc: public ::testing::Test
{
	public:
		TestGuc() {};
		~TestGuc() {};
};

// Mainly test per-db guc via "alter database" which installcheck-good does not seem to cover.
// Need to include other guc test cases (at least installcheck-good: guc.sql/guc.ans)
TEST_F(TestGuc, per_db_guc_with_space)
{
	hawq::test::SQLUtility util(hawq::test::MODE_DATABASE);
	string cmd;

	cmd  = "alter database " + util.getDbName() + " set datestyle to 'sql, mdy'";
	util.execute(cmd);

	util.execSQLFile("catalog/sql/guc.sql", "catalog/ans/guc.ans");
}

