/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>

#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/hdfs_config.h"
#include "lib/file_replace.h"
#include "test_hawq_extract.h"

#include "gtest/gtest.h"

using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;
using hawq::test::HdfsConfig;

TEST_F(TestHawqExtract, TestExtractAfterReorganize) {
    SQLUtility util;
    util.execute("drop table if exists table_extract_ao;");
    util.execute("drop table if exists table_extract_parquet;");
    util.execute("drop table if exists table_extract_ao_new;");
    util.execute("drop table if exists table_extract_parquet_new;");
    // create an ao and a parquet table and insert data
    util.execute("CREATE TABLE table_extract_ao(id int);");
    util.execute("CREATE TABLE table_extract_parquet(id int) WITH (APPENDONLY=true, ORIENTATION=parquet);");
    util.execute("insert into table_extract_ao values(1),(2),(3);");
    util.execute("insert into table_extract_parquet values(1),(2),(3);");

    // reorganize table
    util.execute("alter table table_extract_ao set with (reorganize=true);");
    util.execute("alter table table_extract_parquet set with (reorganize=true);");

    // extract table to .yml
    EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o table_extract_ao.yml testhawqextract_testextractafterreorganize.table_extract_ao"));
    EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o table_extract_parquet.yml testhawqextract_testextractafterreorganize.table_extract_parquet"));

    // register .yml to new table
    EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c table_extract_ao.yml testhawqextract_testextractafterreorganize.table_extract_ao_new"));
    EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c table_extract_parquet.yml testhawqextract_testextractafterreorganize.table_extract_parquet_new"));
    util.query("select * from table_extract_ao_new;", 3);
    util.query("select * from table_extract_parquet_new;", 3);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf table_extract_ao.yml")));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf table_extract_parquet.yml")));
    util.execute("drop table table_extract_ao;");
    util.execute("drop table table_extract_parquet;");
    util.execute("drop table table_extract_ao_new;");
    util.execute("drop table table_extract_parquet_new;");
}

TEST_F(TestHawqExtract, TestExtractAfterTruncate) {
    SQLUtility util;
    util.execute("drop table if exists table_extract_ao;");
    util.execute("drop table if exists table_extract_parquet;");
    util.execute("drop table if exists table_extract_ao_new;");
    util.execute("drop table if exists table_extract_parquet_new;");
    // create an ao and a parquet table and insert data
    util.execute("CREATE TABLE table_extract_ao(id int);");
    util.execute("CREATE TABLE table_extract_parquet(id int) WITH (APPENDONLY=true, ORIENTATION=parquet);");
    util.execute("insert into table_extract_ao values(1),(2),(3);");
    util.execute("insert into table_extract_parquet values(1),(2),(3);");

    // truncate table and insert again
    util.execute("TRUNCATE table table_extract_ao;");
    util.execute("TRUNCATE table table_extract_parquet;");
    util.execute("insert into table_extract_ao values(1),(2),(3);");
    util.execute("insert into table_extract_parquet values(1),(2),(3);");

    // extract table
    EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o table_extract_ao.yml testhawqextract_testextractaftertruncate.table_extract_ao"));
    EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o table_extract_parquet.yml testhawqextract_testextractaftertruncate.table_extract_parquet"));

    // register .yml to new table
    EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c table_extract_ao.yml testhawqextract_testextractaftertruncate.table_extract_ao_new"));
    EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c table_extract_parquet.yml testhawqextract_testextractaftertruncate.table_extract_parquet_new"));
    util.query("select * from table_extract_ao_new;", 3);
    util.query("select * from table_extract_parquet_new;", 3);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf table_extract_ao.yml")));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf table_extract_parquet.yml")));
    util.execute("drop table table_extract_ao;");
    util.execute("drop table table_extract_parquet;");
    util.execute("drop table table_extract_ao_new;");
    util.execute("drop table table_extract_parquet_new;");
}
