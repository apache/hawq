#include "gtest/gtest.h"

#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/file_replace.h"
#include "lib/hawq_scp.h"


class TestUDF: public ::testing::Test
{
	public:
		TestUDF() {}
		~TestUDF() {}
};

TEST_F(TestUDF, TestUDFBasics)
{
	hawq::test::SQLUtility util;
	if (util.getGUCValue("optimizer") == "on")
	{
		util.execSQLFile("UDF/sql/function_basics.sql",
		                 "UDF/ans/function_basics.ans.orca");
	}
	else
	{
		util.execSQLFile("UDF/sql/function_basics.sql",
		                 "UDF/ans/function_basics.ans.planner");
	}
}

TEST_F(TestUDF, TestUDFCreation)
{
	// preprocess source files to get sql/ans files
	hawq::test::SQLUtility util;
	std::string d_feature_test_root(util.getTestRootPath());
	std::string f_sql_tpl(d_feature_test_root + "/UDF/sql/function_creation.sql.source");
	std::string f_ans_tpl(d_feature_test_root + "/UDF/ans/function_creation.ans.source");
	std::string f_sql(d_feature_test_root + "/UDF/sql/function_creation.sql");
	std::string f_ans(d_feature_test_root + "/UDF/ans/function_creation.ans");

	hawq::test::FileReplace frep;
	std::unordered_map<std::string, std::string> strs_src_dst;
	strs_src_dst["@SHARE_LIBRARY_PATH@"] = d_feature_test_root + "/UDF/lib/function.so";

	frep.replace(f_sql_tpl, f_sql, strs_src_dst);
	frep.replace(f_ans_tpl, f_ans, strs_src_dst);

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("UDF/sql/function_creation.sql",
	                 "UDF/ans/function_creation.ans");
}

TEST_F(TestUDF, TestUDFSetReturning)
{
	hawq::test::SQLUtility util;
	// enable plpythonu language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plpythonu'") != "plpythonu")
	{
		util.execute("CREATE LANGUAGE plpythonu", false);
	}

	// run test if plpythonu language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plpythonu'") == "plpythonu")
	{
		util.execSQLFile("UDF/sql/function_set_returning.sql",
		                 "UDF/ans/function_set_returning.ans");
	}
}

TEST_F(TestUDF, TestUDFExtension)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("UDF/sql/function_extension.sql",
	                 "UDF/ans/function_extension.ans");
}

TEST_F(TestUDF, TestUDFInternal)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("UDF/sql/function_internal.sql",
	                 "UDF/ans/function_internal.ans");
}

TEST_F(TestUDF, TestUDFC)
{
	// preprocess source files to get sql/ans files
	hawq::test::SQLUtility util;
	std::string d_feature_test_root(util.getTestRootPath());
	std::string f_sql_tpl(d_feature_test_root + "/UDF/sql/function_c.sql.source");
	std::string f_ans_tpl(d_feature_test_root + "/UDF/ans/function_c.ans.source");
	std::string f_sql(d_feature_test_root + "/UDF/sql/function_c.sql");
	std::string f_ans(d_feature_test_root + "/UDF/ans/function_c.ans");

	hawq::test::FileReplace frep;
	std::unordered_map<std::string, std::string> strs_src_dst;
	strs_src_dst["@SHARE_LIBRARY_PATH@"] = d_feature_test_root + "/UDF/lib/function.so";

	frep.replace(f_sql_tpl, f_sql, strs_src_dst);
	frep.replace(f_ans_tpl, f_ans, strs_src_dst);

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("UDF/sql/function_c.sql",
	                 "UDF/ans/function_c.ans");
}

TEST_F(TestUDF, TestUDFSql)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("UDF/sql/function_sql.sql",
	                 "UDF/ans/function_sql.ans");
}

TEST_F(TestUDF, TestUDFPlpgsql)
{
	hawq::test::SQLUtility util;
	// enable plpgsql language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plpgsql'") != "plpgsql")
	{
		util.execute("CREATE LANGUAGE plpgsql", false);
	}

	// run test if plpgsql language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plpgsql'") == "plpgsql")
	{
		util.execSQLFile("UDF/sql/function_plpgsql.sql",
		                 "UDF/ans/function_plpgsql.ans");
	}
}

TEST_F(TestUDF, TestUDFPgcrypto)
{
	hawq::test::SQLUtility util;
	// enable pgcrypto package if it is absent
	if (util.getQueryResult("SELECT proname FROM pg_proc WHERE proname = 'crypt'") != "crypt")
	{
		const char *gph = getenv("GPHOME");
		std::string gphome = gph ? gph : "";
		EXPECT_NE(gphome, "");

		util.execSQLFile(gphome + "/share/postgresql/contrib/pgcrypto.sql");
	}

	// run test if pgcrypto package is enabled
	if (util.getQueryResult("SELECT proname FROM pg_proc WHERE proname = 'crypt'") == "crypt")
	{
		util.execSQLFile("UDF/sql/function_pgcrypto.sql",
		                 "UDF/ans/function_pgcrypto.ans");
	}
}

TEST_F(TestUDF, TestUDFPlr)
{
	hawq::test::SQLUtility util;
	// enable plr language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plr'") != "plr")
	{
		util.execute("CREATE LANGUAGE plr", false);
	}

	// run test if plr language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plr'") == "plr")
	{
		util.execSQLFile("UDF/sql/function_plr.sql",
		                 "UDF/ans/function_plr.ans");
	}
}

TEST_F(TestUDF, TestUDFPljava)
{
	hawq::test::SQLUtility util;
	std::string d_feature_test_root(util.getTestRootPath());

	const char *gph = getenv("GPHOME");
	std::string gphome = gph ? gph : "";
	EXPECT_NE(gphome, "");

	// enable pljava language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'java'") != "java")
	{
		hawq::test::Command cmd("psql -f " + gphome + "/share/postgresql/pljava/install.sql");
		cmd.run();
	}

	// run test if pljava language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'java'") == "java")
	{
		// copy jar files over hawq cluster
		std::string query = "SELECT string_agg('-h ' || hostname, ' ' ORDER BY hostname) FROM gp_segment_configuration;";
		std::string hosts = util.getQueryResult(query);
		hawq::test::HAWQScp hscp;
		EXPECT_EQ(hscp.copy(hosts, d_feature_test_root + "/UDF/sql/PLJavaAdd.jar", gphome + "/lib/postgresql/java/"), true);

		util.execSQLFile("UDF/sql/function_pljava.sql",
		                 "UDF/ans/function_pljava.ans");
	}
}

TEST_F(TestUDF, TestUDFPljavau)
{
	hawq::test::SQLUtility util;
	std::string d_feature_test_root(util.getTestRootPath());

	const char *gph = getenv("GPHOME");
	std::string gphome = gph ? gph : "";
	EXPECT_NE(gphome, "");

	// enable pljavau language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'javau'") != "javau")
	{
		hawq::test::Command cmd("psql -f " + gphome + "/share/postgresql/pljava/install.sql");
		cmd.run();
	}

	// run test if pljavau language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'javau'") == "javau")
	{
		// copy jar files over hawq cluster
		std::string query = "SELECT string_agg('-h ' || hostname, ' ' ORDER BY hostname) FROM gp_segment_configuration;";
		std::string hosts = util.getQueryResult(query);
		hawq::test::HAWQScp hscp;
		EXPECT_EQ(hscp.copy(hosts, d_feature_test_root + "/UDF/sql/PLJavauAdd.jar", gphome + "/lib/postgresql/java/"), true);

		util.execSQLFile("UDF/sql/function_pljavau.sql",
		                 "UDF/ans/function_pljavau.ans");
	}
}

TEST_F(TestUDF, TestUDFPlpythonu)
{
	hawq::test::SQLUtility util;
	// enable plpythonu language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plpythonu'") != "plpythonu")
	{
		util.execute("CREATE LANGUAGE plpythonu", false);
	}

	// run test if plpythonu language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plpythonu'") == "plpythonu")
	{
		util.execSQLFile("UDF/sql/function_plpythonu.sql",
		                 "UDF/ans/function_plpythonu.ans");
	}
}

TEST_F(TestUDF, TestUDFPlperl)
{
	hawq::test::SQLUtility util;
	// enable plperl language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plperl'") != "plperl")
	{
		util.execute("CREATE LANGUAGE plperl", false);
	}

	// run test if plperl language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plperl'") == "plperl")
	{
		util.execSQLFile("UDF/sql/function_plperl.sql",
		                 "UDF/ans/function_plperl.ans");
	}
}

TEST_F(TestUDF, TestUDFPlperlu)
{
	hawq::test::SQLUtility util;
	// enable plperlu language if it is absent
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plperlu'") != "plperlu")
	{
		util.execute("CREATE LANGUAGE plperlu", false);
	}

	// run test if plperlu language is enabled
	if (util.getQueryResult("SELECT lanname FROM pg_language WHERE lanname = 'plperlu'") == "plperlu")
	{
		util.execSQLFile("UDF/sql/function_plperlu.sql",
		                 "UDF/ans/function_plperlu.ans");
	}
}
