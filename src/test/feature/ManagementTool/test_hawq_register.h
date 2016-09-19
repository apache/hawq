#ifndef TEST_HAWQ_REGISTER_H
#define TEST_HAWQ_REGISTER_H

#include <string>
#include <pwd.h>
#include "lib/hdfs_config.h"
#include "gtest/gtest.h"

class TestHawqRegister : public ::testing::Test {
    public:
        TestHawqRegister() {
            std::string user = HAWQ_USER;
            if(user.empty()) {
                struct passwd *pw;
                uid_t uid = geteuid();
                pw = getpwuid(uid);
                user.assign(pw->pw_name);
            }
            conn.reset(new hawq::test::PSQL(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, user, HAWQ_PASSWORD));
        }
        ~TestHawqRegister() {}

        std::string getHdfsLocation() {
            hawq::test::HdfsConfig hc;
            std::string namenodehost = "";
            EXPECT_EQ(true, hc.getNamenodeHost(namenodehost));
            return hawq::test::stringFormat("hdfs://%s", namenodehost.c_str());
        }

        std::string getDatabaseOid() {
            const hawq::test::PSQLQueryResult &result = conn->getQueryResult(
                hawq::test::stringFormat("SELECT oid from pg_database where datname = \'%s\';", HAWQ_DB));
            std::vector<std::vector<std::string>> table = result.getRows();
            if (table.size() > 0) {
                return table[0][0];
            }

            return "";
        }

        std::string getTableOid(std::string relname) {
            const hawq::test::PSQLQueryResult &result = conn->getQueryResult(
                hawq::test::stringFormat("SELECT oid from pg_class where relname = \'%s\';", relname.c_str()));
            std::vector<std::vector<std::string>> table = result.getRows();
            if (table.size() > 0) {
                return table[0][0];
            }

            return "";
        }

    private:
        std::unique_ptr<hawq::test::PSQL> conn;
};

#endif
