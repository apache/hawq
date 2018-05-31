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
#include "gtest/gtest.h"
#include "lib/command.h"
#include "lib/hawq_config.h"
#include "lib/sql_util.h"

using hawq::test::SQLUtility;
using std::string;
using hawq::test::Command;

class TestHashJoinBloomFilter: public ::testing::Test
{
    public:
        TestHashJoinBloomFilter() {}
        ~TestHashJoinBloomFilter() {}
};

TEST_F(TestHashJoinBloomFilter, BasicTest)
{
    SQLUtility util;
    hawq::test::HawqConfig hawq_config;
    util.execute("drop table if exists fact;");
    util.execute("create table fact(c1 int, c3 int) WITH(appendonly=true, ORIENTATION=parquet) distributed by (c1);");
    util.execute("insert into fact select generate_series(1, 300000), generate_series(1, 3000);");
    util.query("select * from fact;", 300000);
    util.execute("drop table if exists dim;");
    util.execute("create table dim(c1 int, c2 int)  distributed by (c1) ;");
    util.execute("insert into dim values(3,1),(1,2),(1,3),(1,1),(2,3),(2,5),(2,6),(1000,7),(2000,1),(3000,2)");
    util.query("select * from dim;", 10);
    util.execSQLFile("query/sql/hashjoin.sql", "query/ans/hashjoin.ans");
    util.execute("set hawq_hashjoin_bloomfilter=true; explain analyze select * from fact, dim where fact.c1 = dim.c1 and dim.c2<4");
    util.execute("drop table dim;");
    util.execute("drop table fact;");
}
