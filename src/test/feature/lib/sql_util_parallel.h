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

/*
 *
 * This is control different sessions sequence issue
 * The file format should be like
     # This is basic transaction test demo    //# is used for comment
     Total:2                                  // indicate how sessions in total
     0:DROP TABLE IF EXISTS TEST;             // 0 indicate it is for prepare
     0:CREATE TABLE TEST(A INT);
     1:BEGIN;
     2:BEGIN;
     1:SELECT * FROM  TEST;
     2:INSERT INTO TEST VALUES(1);
     2:COMMIT;
     1:SELECT * FROM  TEST;
     1:COMMIT;

* There is another example for blocked test
     # for blocked session. in below example,
     # session 1 will lock whole table, session 2 will be blocked until session 1 commit
     Total:2                                  // indicate how sessions in total
     0:DROP TABLE IF EXISTS TEST;             // 0 indicate it is for prepare
     0:CREATE TABLE TEST(A INT);
     1:BEGIN;
     2:BEGIN;
     1:TRUNCATE TEST;
     2:1:SELECT * FROM TEST;               //Here it indicates session 2 is blocked by session 1
     1:COMMIT;                             //Here session 2 will not be blocked and output session 2 result.
     2:COMMIT;
 *
 */
#ifndef HAWQ_SRC_TEST_FEATURE_LIB_SQL_UTIL_PARALL_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_SQL_UTIL_PARALL_H_

#include "sql_util.h"
#include <string>
#include <mutex>

namespace hawq {
namespace test {
 
enum MessageCommandType {
    NULLCOMMAND,
    NEXTLINE,
    BLOCKEDSQL,
    SQLSTRING,
    EXITTHREAD,
    PRINTRESULT,
    BADSTATUS,
    BLOCKOVER
};

class ThreadMessage{
public:
    ThreadMessage(uint16_t tid = 0 );
    ~ThreadMessage();
    int thread_id;
    int vacuum ;
    std::string messageinfo;
    MessageCommandType command;
    bool isrunning;
    int32_t submit; // 1: commit ; 0: submit ; -1: rollback; -2 vacuum;
    std::vector<int> waittids;   //threadids which are blocked by this thread
    std::vector<int> waitfortids; //threadids which this thread waiting for
};

class SQLUtilityParallel : public SQLUtility {
public:
    SQLUtilityParallel();
    ~SQLUtilityParallel();
    bool exclusive; // whether test access_exclusive lock;

  // Execute sql file and diff with ans file.
  // @param sqlFile The given sqlFile which is relative path to test root dir. This file control how the SQL is execute
  // @param ansFile The given ansFile which is relative path to test root dir
  // @param exclusive whether we can predict the order of transactions
  // @return void
  void execParallelControlSQLFile(const std::string &sqlFile, const std::string &ansFile,bool exclusive,
		                          const std::string &initFile = "");
  //int32_t getWaitNumber(const std::string &schemapath, const std::string &connstr);

  // Run sql file and generate output file
    bool runParallelControlFile(const std::string &sqlFile, const std::string &outputFile);
    bool runParallelControlFile_exclusive(const std::string &sqlFile, const std::string &outputFile);
private:

  // Handling SQL comand for every thread
  // @param tid the thread id
  // @param schemapath it is used that every case should have its own schema
  // @param connstr the connection string
  void thread_execute(uint16_t tid, const std::string &schemapath, const std::string &connstr);
  // @param totalsession the number of totalsession
  void thread_execute_exclusive(uint16_t tid, const std::string &schemapath, const std::string &connstr, const int32_t totalsession);

  // Get total session number from input sql file
  int32_t __getTotalSession(std::fstream &in);

  //Process line to get session id, wait tid and running sql
  // @param line  input line
  // @param totalsession the total session for this sql file
  // @param(output) sessionid current execute thread id
  // @param(output) waittid the thread which blocks current thread. 0 if it is not blocked.
  // @param(output) message the sql will be executed
  // @return whether it is executed successfully or not
  bool __processLine(const std::string &line, const int32_t totalsession,
                     int32_t &sessionid, int32_t &waittid, std::string &message);

private:
  std::vector<std::unique_ptr<ThreadMessage>> threadmessage;
  std::mutex thread_mutex;
  FILE* outputstream;
}; // class SQLUtilityParallel

} // namespace test
} // namespace hawq

#endif  // SRC_TEST_FEATURE_LIB_SQL_UTIL_PARALL_H_
