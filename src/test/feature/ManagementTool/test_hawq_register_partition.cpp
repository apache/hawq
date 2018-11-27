#include <string>

#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/hdfs_config.h"
#include "lib/file_replace.h"
#include "test_hawq_register.h"

#include "gtest/gtest.h"

using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;
using hawq::test::HdfsConfig;

void TestHawqRegister::runYamlCaseTableExistsPartition(std::string casename, std::string ymlname, int isexpectederror=1, int checknum=100, int islistpartition=1) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    if (islistpartition > 0) {
        util.execute("CREATE TABLE t (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY LIST (gender) ( PARTITION girls VALUES ('F'), PARTITION boys VALUES ('M'), DEFAULT PARTITION other );");
        util.execute("CREATE TABLE nt (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY LIST (gender) ( PARTITION girls VALUES ('F'), PARTITION boys VALUES ('M'), DEFAULT PARTITION other );");
    } else {
        util.execute("CREATE TABLE t (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY RANGE (id) (PARTITION girls START (1) END (41) EVERY(40), PARTITION boys START (41) END (81) EVERY(40), DEFAULT PARTITION other);");
        util.execute("CREATE TABLE nt (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY RANGE (id) (PARTITION girls START (1) END (41) EVERY(40), PARTITION boys START (41) END (81) EVERY(40), DEFAULT PARTITION other);");
    }
    util.execute("insert into t select generate_series(1, 100), 1, 1, 'F', 1;");
    util.execute("insert into nt select generate_series(1, 100), 1, 1, 'F', 1;");
    util.query("select * from t;", 100);
    util.query("select * from nt;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), ymlname.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), ymlname.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID1@"]= getTableOid("t_1_prt_girls", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID2@"]= getTableOid("t_1_prt_boys", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID3@"]= getTableOid("t_1_prt_other", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    hawq::test::HdfsConfig hc;
    string hdfs_prefix;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(isexpectederror, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str())));
    if (isexpectederror > 0) {
        util.query("select * from t;", 100);
        util.query("select * from nt;", 100);
    } else {
        util.query("select * from nt;", checknum);
        util.execute("insert into nt select generate_series(1, 100), 1, 1, 'M', 1;");
        util.query("select * from nt;", checknum + 100);
    }
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

void TestHawqRegister::runYamlCaseTableNotExistsPartition(std::string casename, std::string ymlname, int isexpectederror=1, int checknum=100, int islistpartition=1) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    if (islistpartition > 0) {
        util.execute("CREATE TABLE t (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY LIST (gender) ( PARTITION girls VALUES ('F'), PARTITION boys VALUES ('M'), DEFAULT PARTITION other );");
    } else {
        util.execute("CREATE TABLE t (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY RANGE (id) (PARTITION girls START (1) END (41) EVERY(40), PARTITION boys START (41) END (81) EVERY(40), DEFAULT PARTITION other);");
    }
    util.execute("insert into t select generate_series(1, 100), 1, 1, 'F', 1;");
    util.query("select * from t;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), ymlname.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), ymlname.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID1@"]= getTableOid("t_1_prt_girls", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID2@"]= getTableOid("t_1_prt_boys", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID3@"]= getTableOid("t_1_prt_other", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    hawq::test::HdfsConfig hc;
    string hdfs_prefix;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(isexpectederror, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str())));
    if (isexpectederror > 0) {
        util.query("select * from t;", 100);
        std::string relnamespace = "2200";
        const hawq::test::PSQLQueryResult &result_tmp = conn->getQueryResult(
            hawq::test::stringFormat("SELECT oid from pg_namespace where nspname= \'testhawqregister_%s\';", casename.c_str()));
        std::vector<std::vector<std::string>> table = result_tmp.getRows();
        if (table.size() > 0) {
            relnamespace = table[0][0];
        }
        util.query(hawq::test::stringFormat("select * from pg_class where relnamespace = %s and relname = 'nt';", relnamespace.c_str()), 0);
    } else {
        util.query("select * from nt;", checknum);
        util.execute("insert into nt select generate_series(1, 100), 1, 1, 'M', 1;");
        util.query("select * from nt;", checknum + 100);
    }
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table if exists nt;");
}

void TestHawqRegister::runYamlCaseForceModePartition(std::string casename, std::string ymlname, int isexpectederror = 1, int rows = 50, int checknum = 200) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), ymlname.c_str()));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), ymlname.c_str()));
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");

    util.execute("CREATE TABLE nt (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY LIST (gender) ( PARTITION girls VALUES ('F'), PARTITION boys VALUES ('M'), DEFAULT PARTITION other );");
    util.execute("insert into nt select generate_series(1, 100), 1, 1, 'F', 1;");
    // get pg_aoseg.pg_xxxseg_xxx table
    std::string reloid1_1_1 = getTableOid("nt_1_prt_girls", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    std::string reloid1_2_1 = getTableOid("nt_1_prt_boys", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    std::string reloid1_3_1 = getTableOid("nt_1_prt_other", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    string result1_1_1 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid1_1_1.c_str()));
    string result1_2_1 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid1_2_1.c_str()));
    string result1_3_1 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid1_3_1.c_str()));
    util.execute("insert into nt select generate_series(1, 100), 1, 1, 'F', 1;");
    util.query("select * from nt;", 200);

    // hawq register --force -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
    util.execute("CREATE TABLE t (id int, rank int, year int, gender char(1), count int ) DISTRIBUTED BY (id) PARTITION BY LIST (gender) ( PARTITION girls VALUES ('F'), PARTITION boys VALUES ('M'), DEFAULT PARTITION other );");
    util.execute(hawq::test::stringFormat("insert into t select generate_series(1, %s), 1, 1, 'F', 1;", std::to_string(rows).c_str()));
    // get pg_aoseg.pg_xxxseg_xxx table
    std::string reloid1_1_2 = getTableOid("t_1_prt_girls", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    std::string reloid1_2_2 = getTableOid("t_1_prt_boys", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    std::string reloid1_3_2 = getTableOid("t_1_prt_other", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    string result1_1_2 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid1_1_2.c_str()));
    string result1_2_2 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid1_2_2.c_str()));
    string result1_3_2 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid1_3_2.c_str()));
    util.query("select * from t;", rows);
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID_OLD@"]= getTableOid("nt", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_NEW@"]= getTableOid("t", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_OLD1@"]= getTableOid("nt_1_prt_girls", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_OLD2@"]= getTableOid("nt_1_prt_boys", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_OLD3@"]= getTableOid("nt_1_prt_other", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_NEW1@"]= getTableOid("t_1_prt_girls", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_NEW2@"]= getTableOid("t_1_prt_boys", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_NEW3@"]= getTableOid("t_1_prt_other", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    
    EXPECT_EQ(isexpectederror, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str())));
    util.query("select * from nt;", checknum);

    // check pg_aoseg.pg_xxxseg_xxx table
    std::string reloid2_1 = getTableOid("nt_1_prt_girls", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    std::string reloid2_2 = getTableOid("nt_1_prt_boys", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    std::string reloid2_3 = getTableOid("nt_1_prt_other", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    string result2_1 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid2_1.c_str()));
    string result2_2 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid2_2.c_str()));
    string result2_3 = util.getQueryResultSetString(hawq::test::stringFormat("select eof, tupcount, varblockcount, eofuncompressed from pg_aoseg.pg_aoseg_%s order by segno;", reloid2_3.c_str()));

    result1_1_1.substr(0, result1_1_1.length() - 1);
    result1_2_1.substr(0, result1_1_1.length() - 1);
    result1_3_1.substr(0, result1_1_1.length() - 1);
    EXPECT_EQ(result1_1_1 + result1_1_2, result2_1);
    EXPECT_EQ(result1_2_1 + result1_2_2, result2_2);
    EXPECT_EQ(result1_3_1 + result1_3_2, result2_3);

    util.execute("insert into nt select generate_series(1, 100), 1, 1, 'M', 1;");
    util.query("select * from nt;", checknum + 100);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestPartitionTableNotExistsListPartition) {
    runYamlCaseTableNotExistsPartition("testpartitiontablenotexistslistpartition", "partition/table_not_exists_list_partition", 0, 100);
}

TEST_F(TestHawqRegister, TestPartitionTableNotExistsRangePartition) {
    runYamlCaseTableNotExistsPartition("testpartitiontablenotexistsrangepartition", "partition/table_not_exists_range_partition", 0, 100, 0);
}

TEST_F(TestHawqRegister, TestPartitionTableExistsListPartition) {
    runYamlCaseTableExistsPartition("testpartitiontableexistslistpartition", "partition/table_exists_list_partition", 0, 200);
}

TEST_F(TestHawqRegister, TestPartitionTableExistsRangePartition) {
    runYamlCaseTableExistsPartition("testpartitiontableexistsrangepartition", "partition/table_exists_range_partition", 0, 200, 0);
}

TEST_F(TestHawqRegister, TestPartitionForceModeNormal) {
    runYamlCaseForceModePartition("testpartitionforcemodenormal", "partition/force_mode_normal", 0, 50, 150); 
}

TEST_F(TestHawqRegister, TestPartitionSubSetCatalogTable) {
    runYamlCaseTableExistsPartition("testpartitionsubsetcatalogtable", "partition/sub_set_catalog_table", 0, 200);
}

TEST_F(TestHawqRegister, TestPartitionDifferentPartitionPolicy) {
    runYamlCaseTableExistsPartition("testpartitiondifferentpartitionpolicy", "partition/different_partition_policy");
}

TEST_F(TestHawqRegister, TestPartitionConstraintNotExistsInCatalog) {
    runYamlCaseTableExistsPartition("testpartitionconstraintnotexistsincatalog", "partition/constraint_not_exists_in_catalog");
}

TEST_F(TestHawqRegister, TestPartitionDuplicatePartitionConstraint) {
    runYamlCaseTableExistsPartition("testpartitionduplicatepartitionconstraint", "partition/duplicate_partition_constraint");
}

TEST_F(TestHawqRegister, TestPartitionTableNotExistsTableFileNotExists) {
    runYamlCaseTableNotExistsPartition("testpartitiontablenotexiststablefilenotexists", "partition/table_not_exists_table_file_not_exists");
}

TEST_F(TestHawqRegister, TestPartitionTableExistsTableFileNotExists) {
    runYamlCaseTableExistsPartition("testpartitiontableexiststablefilenotexists", "partition/table_exists_table_file_not_exists");
}

TEST_F(TestHawqRegister, TestPartitionTableMultilevel) {
    SQLUtility util;
    util.execute("drop table if exists sales;");
    util.execute("drop table if exists nsales;");
    // create a partition table and extract it
    util.execute("CREATE TABLE sales (trans_id int, date date, region text) DISTRIBUTED BY (trans_id) PARTITION BY RANGE (date) \ "
                "(START (date '2011-01-01') INCLUSIVE END (date '2011-05-01') EXCLUSIVE EVERY (INTERVAL '1 month'), DEFAULT PARTITION outlying_dates );");
    util.execute("insert into sales values(1, '2011-1-15', 'usa');");

    EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o sales.yml testhawqregister_testpartitiontablemultilevel.sales"));

    // create a two-level partition table
    util.execute("CREATE TABLE nsales (trans_id int, date date, region text) DISTRIBUTED BY (trans_id) PARTITION BY RANGE (date) \ "
                "SUBPARTITION BY LIST (region) SUBPARTITION TEMPLATE ( SUBPARTITION usa VALUES ('usa'), SUBPARTITION asia VALUES ('asia'),\ "
                "SUBPARTITION europe VALUES ('europe'), DEFAULT SUBPARTITION other_regions) (START (date '2011-01-01') INCLUSIVE END (date '2011-06-01') EXCLUSIVE \ "
                "EVERY (INTERVAL '1 month'), DEFAULT PARTITION outlying_dates );");

    // should fail since multi-level partition is not supported
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c sales.yml testhawqregister_testpartitiontablemultilevel.nsales"));

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf sales.yml")));
    util.execute("drop table nsales;");
    util.execute("drop table sales;");
}
