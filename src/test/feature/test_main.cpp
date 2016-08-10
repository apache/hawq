#include <sys/types.h>
#include <pwd.h>
#include "gtest/gtest.h"
#include "psql.h"
#include "sql_util.h"

using std::string;

class TestPrepare
{
  private:
    const string testDbName = "hawq_feature_test";
    std::unique_ptr<hawq::test::PSQL> conn;
	void init_hawq_test();
  public:
    TestPrepare();
    ~TestPrepare();
};

#define PSQL_RUN_AND_ASSERT() \
  conn->runSQLCommand(cmd); \
  ASSERT_EQ(0, conn->getLastStatus()) << conn->getLastResult();

void TestPrepare::init_hawq_test()
{
  string user = HAWQ_USER;
  if(user.empty()) {
    struct passwd *pw;
    uid_t uid = geteuid();
    pw = getpwuid(uid);
    user.assign(pw->pw_name);
  }

  conn.reset(new hawq::test::PSQL(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, user, HAWQ_PASSWORD));

  // Create the test db and set some default guc values so that test outputs
  // could be consistent. We do not drop the database in advance since keeping the
  // previous environment could probably help reproducing and resolving some failing
  // test issues, so you need to drop the database yourself when necessary, before
  // running the tests.
  string cmd;
  cmd  = "create database " + testDbName;
  // Do not check return value since probably the db has existed.
  conn->runSQLCommand(cmd);
  cmd  = "alter database " + testDbName + " set lc_messages to 'C'";
  PSQL_RUN_AND_ASSERT();
  cmd  = "alter database " + testDbName + " set lc_monetary to 'C'";
  PSQL_RUN_AND_ASSERT();
  cmd  = "alter database " + testDbName + " set lc_numeric to 'C'";
  PSQL_RUN_AND_ASSERT();
  cmd  = "alter database " + testDbName + " set lc_time to 'C'";
  PSQL_RUN_AND_ASSERT();
  cmd  = "alter database " + testDbName + " set timezone_abbreviations to 'Default'";
  PSQL_RUN_AND_ASSERT();
  cmd  = "alter database " + testDbName + " set timezone to 'PST8PDT'";
  PSQL_RUN_AND_ASSERT();
  cmd  = "alter database " + testDbName + " set datestyle to 'postgres,MDY'";
  PSQL_RUN_AND_ASSERT();
}

TestPrepare::TestPrepare()
{
  init_hawq_test();

  // The test will use the newly created database.
  setenv("PGDATABASE", testDbName.c_str(), 1);
}

TestPrepare::~TestPrepare()
{
}

int main(int argc, char** argv) {

  TestPrepare tp;

  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
