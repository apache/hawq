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

#ifndef TEST_HAWQ_EXTRACT_H
#define TEST_HAWQ_EXTRACT_H

#include <string>
#include <pwd.h>
#include <fstream>
#include "lib/hdfs_config.h"
#include "gtest/gtest.h"

class TestHawqExtract: public ::testing::Test {
    public:
        TestHawqExtract() {
            std::string user = HAWQ_USER;
            if(user.empty()) {
                struct passwd *pw;
                uid_t uid = geteuid();
                pw = getpwuid(uid);
                user.assign(pw->pw_name);
            }
            conn.reset(new hawq::test::PSQL(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, user, HAWQ_PASSWORD));
        }
        ~TestHawqExtract() {}

    private:
        std::unique_ptr<hawq::test::PSQL> conn;
};

#endif
