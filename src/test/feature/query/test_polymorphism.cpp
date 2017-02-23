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
#include <string>

#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestQueryPolymorphism : public ::testing::Test {
 public:
  TestQueryPolymorphism() {}
  ~TestQueryPolymorphism() {}
};


TEST_F(TestQueryPolymorphism, Test1) {
  hawq::test::SQLUtility util;

  // prepare
  util.execute("DROP AGGREGATE IF EXISTS myaggp01a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp02a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp03a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp03b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp04a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp04b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp05a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp06a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp07a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp08a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp09a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp09b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp10a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp10b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp11a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp11b(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp12a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp12b(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp13a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp14a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp15a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp16a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp17a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp17b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp18a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp18b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp19a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp19b(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp20a(anyelement) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggp20b(anyelement) CASCADE");

  util.execute("DROP AGGREGATE IF EXISTS myaggn01a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn01b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn02a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn02b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn03a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn04a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn05a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn05b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn06a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn06b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn07a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn07b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn08a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn08b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn09a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn10a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn11a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn12a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn13a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn13b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn14a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn14b(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn15a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn15b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn16a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn16b(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn17a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn18a(int) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn19a(*) CASCADE");
  util.execute("DROP AGGREGATE IF EXISTS myaggn20a(*) CASCADE");

  util.execute("DROP AGGREGATE IF EXISTS mysum2(anyelement,anyelement) CASCADE");

  util.execute("DROP FUNCTION IF EXISTS stfp(anyarray) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS stfnp(int[]) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS tfp(anyarray,anyelement) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS tfnp(int[],int) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS tf1p(anyarray,int) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS tf2p(int[],anyelement) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS sum3(anyelement,anyelement,anyelement) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS ffp(anyarray) CASCADE");
  util.execute("DROP FUNCTION IF EXISTS ffnp(int[]) CASCADE");

  util.execute("DROP TABLE IF EXISTS t");

  // test starts here

  util.execSQLFile("query/sql/polimorphism-test1.sql",
		  	  	   "query/ans/polimorphism-test1.ans");

  // Legend:
  // -----------
  // A = type is ANY
  // P = type is polymorphic
  // N = type is non-polymorphic
  // B = aggregate base type
  // S = aggregate state type
  // R = aggregate return type
  // 1 = arg1 of a function
  // 2 = arg2 of a function
  // ag = aggregate
  // tf = trans (state) function
  // ff = final function
  // rt = return type of a function
  // -> = implies
  // => = allowed
  // !> = not allowed
  // E  = exists
  // NE = not-exists
  // --
  // Possible states:
  // ----------------
  // B = (A || P || N)
  //   when (B = A) -> (tf2 = NE)
  // S = (P || N)
  // ff = (E || NE)
  // tf1 = (P || N)
  // tf2 = (NE || P || N)
  // R = (P || N)

  // Try to cover all the possible states:
  //
  // Note: in Cases 1 & 2, we are trying to return P. Therefore, if the transfn
  // is stfnp, tfnp, or tf2p, we must use ffp as finalfn, because stfnp, tfnp,
  // and tf2p do not return P. Conversely, in Cases 3 & 4, we are trying to
  // return N. Therefore, if the transfn is stfp, tfp, or tf1p, we must use ffnp
  // as finalfn, because stfp, tfp, and tf1p do not return N.
  //
  //     Case1 (R = P) && (B = A)
  //     ------------------------
  //     S    tf1
  //     -------
  //     N    N

  util.execute("CREATE AGGREGATE myaggp01a(*) "
		       "(SFUNC = stfnp, STYPE = int4[], FINALFUNC = ffp, INITCOND = '{}')");
  //     P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp02a(*) (SFUNC = stfnp, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  //     N    P
  util.execute("CREATE AGGREGATE myaggp03a(*) "
		       "(SFUNC = stfp, STYPE = int4[], FINALFUNC = ffp, INITCOND = '{}')");
  util.execute("CREATE AGGREGATE myaggp03b(*) "
               "(SFUNC = stfp, STYPE = int4[], INITCOND = '{}')");

  //     P    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp04a(*) (SFUNC = stfp, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp04b(*) (SFUNC = stfp, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");

  // --    Case2 (R = P) && ((B = P) || (B = N))
  // --    -------------------------------------
  // --    S    tf1      B    tf2
  // --    -----------------------
  // --    N    N        N    N
  util.execute("CREATE AGGREGATE myaggp05a"
      "(BASETYPE = int, SFUNC = tfnp, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')");
  // --    N    N        N    P
  util.execute("CREATE AGGREGATE myaggp06a"
      "(BASETYPE = int, SFUNC = tf2p, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')");
  // --    N    N        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp07a"
      "(BASETYPE = anyelement, SFUNC = tfnp, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  function tfnp(integer[], anyelement) does not exist");
  // --    N    N        P    P
  util.execute("CREATE AGGREGATE myaggp08a"
               "(BASETYPE = anyelement, SFUNC = tf2p, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')");
  // --    N    P        N    N
  util.execute("CREATE AGGREGATE myaggp09a"
               "(BASETYPE = int, SFUNC = tf1p, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')");
  util.execute("CREATE AGGREGATE myaggp09b"
               "(BASETYPE = int, SFUNC = tf1p, STYPE = int[], INITCOND = '{}')");
  // --    N    P        N    P
  util.execute("CREATE AGGREGATE myaggp10a"
               "(BASETYPE = int, SFUNC = tfp, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')");
  util.execute("CREATE AGGREGATE myaggp10b"
               "(BASETYPE = int, SFUNC = tfp, STYPE = int[], INITCOND = '{}')");
  // --    N    P        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp11a"
      "(BASETYPE = anyelement, SFUNC = tf1p, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  function tf1p(integer[], anyelement) does not exist");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp11b"
      "(BASETYPE = anyelement, SFUNC = tf1p, STYPE = int[], INITCOND = '{}')",
      "ERROR:  function tf1p(integer[], anyelement) does not exist");
  // --    N    P        P    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp12a"
      "(BASETYPE = anyelement, SFUNC = tfp, STYPE = int[], FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  function tfp(integer[], anyelement) does not exist");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp12b"
      "(BASETYPE = anyelement, SFUNC = tfp, STYPE = int[], INITCOND = '{}')",
      "ERROR:  function tfp(integer[], anyelement) does not exist");
  // --    P    N        N    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp13a"
      "(BASETYPE = int, SFUNC = tfnp, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    N        N    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp14a"
      "(BASETYPE = int, SFUNC = tf2p, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    N        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp15a"
      "(BASETYPE = anyelement, SFUNC = tfnp, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  function tfnp(anyarray, anyelement) does not exist");
  // --    P    N        P    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp16a"
      "(BASETYPE = anyelement, SFUNC = tf2p, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  function tf2p(anyarray, anyelement) does not exist");
  // --    P    P        N    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp17a"
      "(BASETYPE = int, SFUNC = tf1p, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp17b"
      "(BASETYPE = int, SFUNC = tf1p, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    P        N    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp18a"
      "(BASETYPE = int, SFUNC = tfp, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");

  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp18b"
      "(BASETYPE = int, SFUNC = tfp, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    P        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp19a"
      "(BASETYPE = anyelement, SFUNC = tf1p, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')",
      "ERROR:  function tf1p(anyarray, anyelement) does not exist");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggp19b"
      "(BASETYPE = anyelement, SFUNC = tf1p, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  function tf1p(anyarray, anyelement) does not exist");
  // --    P    P        P    P
  util.execute("CREATE AGGREGATE myaggp20a"
      "(BASETYPE = anyelement, SFUNC = tfp, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}')");

  util.execute("CREATE AGGREGATE myaggp20b"
      "(BASETYPE = anyelement, SFUNC = tfp, STYPE = anyarray, INITCOND = '{}')");

  // --     Case3 (R = N) && (B = A)
  // --     ------------------------
  // --     S    tf1
  // --     -------
  // --     N    N
  util.execute("CREATE AGGREGATE myaggn01a(*) "
		       "(SFUNC = stfnp, STYPE = int4[], FINALFUNC = ffnp, INITCOND = '{}')");
  util.execute("CREATE AGGREGATE myaggn01b(*) "
		       "(SFUNC = stfnp, STYPE = int4[], INITCOND = '{}');");
  // --     P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn02a(*) "
      "(SFUNC = stfnp, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn02b(*) "
      "(SFUNC = stfnp, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --     N    P
  util.execute("CREATE AGGREGATE myaggn03a(*) "
		       "(SFUNC = stfp, STYPE = int4[], FINALFUNC = ffnp, INITCOND = '{}')");
  // --     P    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn04a(*) "
      "(SFUNC = stfp, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");

  // --    Case4 (R = N) && ((B = P) || (B = N))
  // --    -------------------------------------
  // --    S    tf1      B    tf2
  // --    -----------------------
  // --    N    N        N    N
  util.execute("CREATE AGGREGATE myaggn05a"
		       "(BASETYPE = int, SFUNC = tfnp, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')");
  util.execute("CREATE AGGREGATE myaggn05b"
               "(BASETYPE = int, SFUNC = tfnp, STYPE = int[], INITCOND = '{}')");
  // --    N    N        N    P
  util.execute("CREATE AGGREGATE myaggn06a"
		       "(BASETYPE = int, SFUNC = tf2p, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')");
  util.execute("CREATE AGGREGATE myaggn06b"
		       "(BASETYPE = int, SFUNC = tf2p, STYPE = int[], INITCOND = '{}')");
  // --    N    N        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn07a "
      "(BASETYPE = anyelement, SFUNC = tfnp, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  function tfnp(integer[], anyelement) does not exist");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn07b "
      "(BASETYPE = anyelement, SFUNC = tfnp, STYPE = int[], INITCOND = '{}')",
      "ERROR:  function tfnp(integer[], anyelement) does not exist");
  // --    N    N        P    P
  util.execute("CREATE AGGREGATE myaggn08a"
               "(BASETYPE = anyelement, SFUNC = tf2p, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')");
  util.execute("CREATE AGGREGATE myaggn08b"
		       "(BASETYPE = anyelement, SFUNC = tf2p, STYPE = int[], INITCOND = '{}')");
  // --    N    P        N    N
  util.execute("CREATE AGGREGATE myaggn09a"
		       "(BASETYPE = int, SFUNC = tf1p, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')");
  // --    N    P        N    P
  util.execute("CREATE AGGREGATE myaggn10a"
		       "(BASETYPE = int, SFUNC = tfp, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')");
  // --    N    P        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn11a"
      "(BASETYPE = anyelement, SFUNC = tf1p, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  function tf1p(integer[], anyelement) does not exist");
  // --    N    P        P    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn12a"
      "(BASETYPE = anyelement, SFUNC = tfp, STYPE = int[], FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  function tfp(integer[], anyelement) does not exist");
  // --    P    N        N    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn13a"
      "(BASETYPE = int, SFUNC = tfnp, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn13b"
      "(BASETYPE = int, SFUNC = tfnp, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    N        N    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn14a"
      "(BASETYPE = int, SFUNC = tf2p, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn14b"
      "(BASETYPE = int, SFUNC = tf2p, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    N        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn15a"
      "(BASETYPE = anyelement, SFUNC = tfnp, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  function tfnp(anyarray, anyelement) does not exist");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn15b"
      "(BASETYPE = anyelement, SFUNC = tfnp, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  function tfnp(anyarray, anyelement) does not exist");
  // --    P    N        P    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn16a"
      "(BASETYPE = anyelement, SFUNC = tf2p, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  function tf2p(anyarray, anyelement) does not exist");
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn16b"
      "(BASETYPE = anyelement, SFUNC = tf2p, STYPE = anyarray, INITCOND = '{}')",
      "ERROR:  function tf2p(anyarray, anyelement) does not exist");
  // --    P    P        N    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn17a"
      "(BASETYPE = int, SFUNC = tf1p, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    P        N    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn18a"
      "(BASETYPE = int, SFUNC = tfp, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  cannot determine transition data type");
  // --    P    P        P    N
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn19a"
      "(BASETYPE = anyelement, SFUNC = tf1p, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  function tf1p(anyarray, anyelement) does not exist");
  // --    P    P        P    P
  util.executeExpectErrorMsgStartWith(
      "CREATE AGGREGATE myaggn20a"
      "(BASETYPE = anyelement, SFUNC = tfp, STYPE = anyarray, FINALFUNC = ffnp, INITCOND = '{}')",
      "ERROR:  function ffnp(anyarray) does not exist");

  // multi-arg polymorphic
  util.execute("CREATE AGGREGATE mysum2(anyelement,anyelement) "
		       "(SFUNC = sum3, STYPE = anyelement, INITCOND = '0')");

  util.execute("CREATE TABLE t (f1 int, f2 int[], f3 text)");
  util.execute("INSERT INTO t VALUES(1,array[1],'a')");
  util.execute("INSERT INTO t VALUES(2,array[11],'b')");
  util.execute("INSERT INTO t VALUES(3,array[111],'c')");
  util.execute("INSERT INTO t VALUES(4,array[2],'a')");
  util.execute("INSERT INTO t VALUES(5,array[22],'b')");
  util.execute("INSERT INTO t VALUES(6,array[222],'c')");
  util.execute("INSERT INTO t VALUES(7,array[3],'a')");
  util.execute("INSERT INTO t VALUES(8,array[3],'b')");

  util.execSQLFile("query/sql/polimorphism-test1-query.sql",
		  	  	   "query/ans/polimorphism-test1-query.ans");

  // clean up
  util.execute("DROP TABLE t");

  util.execute("DROP AGGREGATE myaggp01a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp02a(*)");
  util.execute("DROP AGGREGATE myaggp03a(*)");
  util.execute("DROP AGGREGATE myaggp03b(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp04a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp04b(*)");
  util.execute("DROP AGGREGATE myaggp05a(int)");
  util.execute("DROP AGGREGATE myaggp06a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp07a(anyelement)");
  util.execute("DROP AGGREGATE myaggp08a(anyelement)");
  util.execute("DROP AGGREGATE myaggp09a(int)");
  util.execute("DROP AGGREGATE myaggp09b(int)");
  util.execute("DROP AGGREGATE myaggp10a(int)");
  util.execute("DROP AGGREGATE myaggp10b(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp11a(anyelement)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp11b(anyelement)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp12a(anyelement)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp12b(anyelement)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp13a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp14a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp15a(anyelement)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp16a(anyelement)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp17a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp17b(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp18a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp18b(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp19a(anyelement)");
  util.execute("DROP AGGREGATE IF EXISTS myaggp19b(anyelement)");
  util.execute("DROP AGGREGATE myaggp20a(anyelement)");
  util.execute("DROP AGGREGATE myaggp20b(anyelement)");

  util.execute("DROP AGGREGATE myaggn01a(*)");
  util.execute("DROP AGGREGATE myaggn01b(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn02a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn02b(*)");
  util.execute("DROP AGGREGATE myaggn03a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn04a(*)");
  util.execute("DROP AGGREGATE myaggn05a(int)");
  util.execute("DROP AGGREGATE myaggn05b(int)");
  util.execute("DROP AGGREGATE myaggn06a(int)");
  util.execute("DROP AGGREGATE myaggn06b(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn07a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn07b(*)");
  util.execute("DROP AGGREGATE myaggn08a(anyelement)");
  util.execute("DROP AGGREGATE myaggn08b(anyelement)");
  util.execute("DROP AGGREGATE myaggn09a(int)");
  util.execute("DROP AGGREGATE myaggn10a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn11a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn12a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn13a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn13b(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn14a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn14b(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn15a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn15b(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn16a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn16b(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn17a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn18a(int)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn19a(*)");
  util.execute("DROP AGGREGATE IF EXISTS myaggn20a(*)");

  util.execute("DROP AGGREGATE mysum2(anyelement,anyelement)");

  util.execute("DROP FUNCTION IF EXISTS stfp(anyarray)");
  util.execute("DROP FUNCTION IF EXISTS stfnp(int[])");
  util.execute("DROP FUNCTION IF EXISTS tfp(anyarray,anyelement)");
  util.execute("DROP FUNCTION IF EXISTS tfnp(int[],int)");
  util.execute("DROP FUNCTION IF EXISTS tf1p(anyarray,int)");
  util.execute("DROP FUNCTION IF EXISTS tf2p(int[],anyelement)");
  util.execute("DROP FUNCTION IF EXISTS sum3(anyelement,anyelement,anyelement)");
  util.execute("DROP FUNCTION IF EXISTS ffp(anyarray)");
  util.execute("DROP FUNCTION IF EXISTS ffnp(int[])");
}

TEST_F(TestQueryPolymorphism, Test2) {
  hawq::test::SQLUtility util;
}
