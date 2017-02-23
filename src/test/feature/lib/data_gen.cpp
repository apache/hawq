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
#include "data_gen.h"
using std::string;

namespace hawq {
namespace test {

void DataGenerator::genSimpleTable(string tableName, bool appendonly,
                                   string orientation,
                                   string compresstype,
                                   int compresslevel) {
  string desc =
      genTableDesc(appendonly, orientation, compresstype, compresslevel);
  string createSql =
      "create table " + tableName + "(a int, b int) " + desc;
  sqlUtil->execute(createSql);

  string insertSql =
      "insert into " + tableName + " values(51,62), (14,15), (1,3);";
  sqlUtil->execute(insertSql);
}

void DataGenerator::genAggregateTable(string tableName, bool appendonly,
                                   string orientation,
                                   string compresstype,
                                   int compresslevel) {
  string desc =
      genTableDesc(appendonly, orientation, compresstype, compresslevel);
  string createSql =
      "create table " + tableName + "(a int, b VARCHAR, c int) " + desc;
  sqlUtil->execute(createSql);

  string insertSql =
      "insert into " + tableName + " values(1, 'aa', 10), (1, 'bb', 20), "
          "(2, 'cc', 20), (3, 'cc', 20);";
  sqlUtil->execute(insertSql);
}

void DataGenerator::genTableWithSeries(string tableName, bool appendonly,
                                       string orientation,
                                       string compresstype,
                                       int compresslevel) {
  string desc =
      genTableDesc(appendonly, orientation, compresstype, compresslevel);
  string createSql =
      "create table " + tableName + "(a int, b varchar(20)) " + desc;
  sqlUtil->execute(createSql);

  string insertSql =
      "insert into " + tableName + " values(generate_series(1,10000), 'abc')";
  sqlUtil->execute(insertSql);
}

void DataGenerator::genTableWithFullTypes(string tableName,
                                          bool appendonly,
                                          string orientation,
                                          string compresstype,
                                          int compresslevel) {
  string desc =
      genTableDesc(appendonly, orientation, compresstype, compresslevel);
  string createSql =
      "create table " + tableName +
      "(c0 int4, c1 polygon, "
      "c2 text, c3 time, c4 timetz, c5 macaddr, c6 timestamptz, c7 char(10), "
      "c8 int2, c9 bool, c10 cidr, c11 circle, c12 lseg, c13 interval, "
      "c14 bit, c15 money, c16 box, c17 bytea, c18 xml, c19 bit(5), "
      "c20 varchar(10), c21 inet, c22 int8, c23 varbit, c24 serial, "
      "c25 float4, c26 point, c27 date, c28 float8) " +
      desc;
  sqlUtil->execute(createSql);

  string insertSql =
      "insert into " + tableName +
      " values (2147483647, null, null, '00:00:00', null, 'FF:89:71:45:AE:01',"
      " '2000-01-01 08:00:00+09', null, 32767, 'true', '192.168.1.255/32', "
      "'<(1,2),3>', '[(0,0),(6,6)]', '-178000000 years', '0', '-21474836.48', "
      "'((100,200),(200,400))', null, '<aa>bb</aa>', null, '123456789a', "
      "'2001:db8:85a3:8d3:1319:8a2e:370:7344/64', null, null, 1, 0, POINT(1,2),"
      " '4277-12-31 AD', 128);";
  sqlUtil->execute(insertSql);

  string insertSql2 =
      "insert into " + tableName +
      " values (0, '((100,123),(5,10),(7,2),(4,5))', 'hello world', null, "
      "'04:45:05.0012+08:40', null, null, 'bbccddeeff', 128, null, "
      "'2001:db8:85a3:8d3:1319:8a2e:370:7344/128', '<(1,2),3>', "
      "'[(0,0),(6,6)]',"
      " null, '1', '0', '((0,1),(2,3))', 'hello world', '<aa>bb</aa>', null, "
      "'aaaa', '2001:db8:85a3:8d3:1319:8a2e:370:7344/64', 0, null, 2147483647, "
      "'-Infinity', POINT(1,2), '4277-12-31 AD', 'Infinity');";
  sqlUtil->execute(insertSql2);

  string insertSql3 =
      "insert into " + tableName +
      " values (null, null, 'abcd', '15:01:03', null, null, "
      " '2000-01-01 08:00:00+09', null, null, 'true', null, "
      "null, '[(0,0),(6,6)]', '-178000000 years', '0', '-21474836.48', "
      "'((100,200),(200,400))', null, '<aa>bb</aa>', null, '123456789a', "
      "'2001:db8:85a3:8d3:1319:8a2e:370:7344/64', null, null, 1, 0, POINT(1,2),"
      " '4277-12-31 AD', 128);";
  sqlUtil->execute(insertSql3);

  string insertSql4 =
      "insert into " + tableName +
      " values (0, '((100,123),(5,10),(7,2),(4,5))', 'hello world', null, "
      "'04:45:05.0012+08:40', null, null, 'bbccddeeff', 128, null, "
      "'2001:db8:85a3:8d3:1319:8a2e:370:7344/128', '<(1,2),3>', null,"
      " null, null, '0', null, 'hello world', null, null, "
      "'aaaa', '2001:db8:85a3:8d3:1319:8a2e:370:7344/64', 0, null, 2147483647, "
      "'-Infinity', POINT(1,2), '4277-12-31 AD', 'Infinity');";
  sqlUtil->execute(insertSql4);

  string insertSql5 =
      "insert into " + tableName +
      " values (0, '((100,123),(5,10),(7,2),(4,5))', 'hello world', null, "
      "'04:45:05.0012+08:40', null, null, 'bbccddeeff', 128, null, "
      "'2001:db8:85a3:8d3:1319:8a2e:370:7344/128', '<(1,2),3>', null,"
      " null, null, '0', null, 'hello world', null, null, "
      "null, null, 0, null, 2147483647, "
      "'-Infinity', POINT(1,2), '4277-12-31 AD', 'Infinity');";
  sqlUtil->execute(insertSql5);

  string insertSql6 =
      "insert into " + tableName +
      " values (0, '((100,123),(5,10),(7,2),(4,5))', 'hello world', null, "
      "'04:45:05.0012+08:40', null, null, 'bbccddeeff', 128, null, "
      "'2001:db8:85a3:8d3:1319:8a2e:370:7344/128', '<(1,2),3>', null,"
      " null, null, null, null, 'hello world', null, null, "
      "null, null, 0, null, 34, "
      "null, null, null, null);";
  sqlUtil->execute(insertSql6);
}

void DataGenerator::genTableWithNull(string tableName, bool appendonly,
                                     string orientation,
                                     string compresstype,
                                     int compresslevel) {
  string desc =
      genTableDesc(appendonly, orientation, compresstype, compresslevel);
  string createSql =
      "create table " + tableName + " (a int, b float, c varchar(20)) " + desc;
  sqlUtil->execute(createSql);

  string insertSql =
      "insert into " + tableName +
      " values (15, null, 'aa'), (null, null, 'WET'), (null, 51, null);";
  sqlUtil->execute(insertSql);
}

string DataGenerator::genTableDesc(bool appendonly,
                                   string orientation,
                                   string compresstype,
                                   int compresslevel) {
  string desc = (appendonly ? "with (appendonly = true, orientation = "
                                 : "with (appendonly = false, orientation = ") +
                     orientation + ", compresstype = " + compresstype +
                     (compresslevel != -1
                          ? ", compresslevel = " + std::to_string(compresslevel)
                          : "") +
                     ")";
  return desc;
}

} // namespace test
} // namespace hawq
