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

#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <iostream>

#include "lib/command.h"
#include "lib/data_gen.h"
#include "lib/hawq_config.h"
#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestQuerySequence : public ::testing::Test {
 public:
  TestQuerySequence() {}
  ~TestQuerySequence() {}
};

TEST_F(TestQuerySequence, TestSequenceCreateSerialColumn) {
  hawq::test::SQLUtility util;
  bool orcaon = false;
  if (util.getGUCValue("optimizer") == "on") {
	std::cout << "NOTE: TestQuerySequence.TestSequenceCreateSerialColumn "
                 "uses answer file for optimizer on" << std::endl;
    orcaon = true;
  }

  util.execute("drop table if exists serialtest");
  util.execute("create table serialtest (f1 text, f2 serial)");
  util.execute("insert into serialtest values('foo')");
  util.execute("insert into serialtest values('force',100)");
  // expect failure due to null value in serial column
  if (orcaon) {
    util.execSQLFile("query/sql/sequence-serialcol-null.sql",
				     "query/ans/sequence-serialcol-null-orca.ans");
  }
  else {
    util.execSQLFile("query/sql/sequence-serialcol-null.sql",
				     "query/ans/sequence-serialcol-null.ans");

  }
  // query table to check rows with generated and specified values in serial col
  util.execSQLFile("query/sql/sequence-serialcol-query.sql",
		  	  	   "query/ans/sequence-serialcol-query.ans");

  // rename the sequence for that serial column
  util.execute("alter table serialtest_f2_seq rename to serialtest_f2_foo");
  util.execute("insert into serialtest values('more')");

  // query table to check rows
  util.execSQLFile("query/sql/sequence-serialcol-query.sql",
		  	  	   "query/ans/sequence-serialcol-query2.ans");

  // cleanup
  util.execute("drop table serialtest");
}

TEST_F(TestQuerySequence, TestSequenceBasicOperations) {
  hawq::test::SQLUtility util;

	// prepare
	util.execute("drop sequence if exists sequence_test");
	util.execute("create sequence sequence_test");

	// normal nextval operation
	util.query("select nextval('sequence_test'::text)", "1|\n");
	util.query("select nextval('sequence_test'::regclass)", "2|\n");

	// setval with different params
	util.query("select setval('sequence_test'::text, 32)", "32|\n");
	util.query("select nextval('sequence_test'::regclass)", "33|\n");

	util.query("select setval('sequence_test'::text, 99, false)", "99|\n");
	util.query("select nextval('sequence_test'::regclass)", "99|\n");

	util.query("select setval('sequence_test'::regclass, 32)", "32|\n");
	util.query("select nextval('sequence_test'::text)", "33|\n");

	util.query("select setval('sequence_test'::regclass, 99, false)", "99|\n");
	util.query("select nextval('sequence_test'::text)", "99|\n");

	// cleanup
	util.execute("drop sequence sequence_test");
}

TEST_F(TestQuerySequence, TestSequenceRenaming) {
  hawq::test::SQLUtility util;
	// prepare
	util.execute("drop sequence if exists foo_seq");
	util.execute("create sequence foo_seq");
	// alter sequence name
	util.execute("alter table foo_seq rename to foo_seq_new");
	util.query("select * from foo_seq_new",
			   "foo_seq|1|1|9223372036854775807|1|1|1|f|f|\n");
	// cleanup
	util.execute("drop sequence foo_seq_new");
}

TEST_F(TestQuerySequence, TestSequenceDependency) {
  hawq::test::SQLUtility util;
	util.execSQLFile("query/sql/sequence-dependency.sql",
			  	  	 "query/ans/sequence-dependency.ans");
}

TEST_F(TestQuerySequence, TestSequenceAlternate) {
  hawq::test::SQLUtility util;
	// prepare
	util.execute("drop sequence if exists sequence_test2");
	util.execute("create sequence sequence_test2 start with 32");
	util.query("select nextval('sequence_test2')", "32|\n");

	// alter sequence
	util.execute("alter sequence sequence_test2 "
				 	 "restart with 16 "
				 	 "increment by 4 "
				 	 "maxvalue 22 "
				 	 "minvalue 5 "
				 	 "cycle");
	// check the sequence value
	util.query("select nextval('sequence_test2')","16|\n");
	util.query("select nextval('sequence_test2')","20|\n");
	util.query("select nextval('sequence_test2')","5|\n");

	// cleanup
	util.execute("drop sequence sequence_test2");
}



