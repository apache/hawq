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
#include <stdio.h>
#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/hawq_config.h"
#include "test_hawq_reload.h"

#include "gtest/gtest.h"

using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;

/*
Test case for hawq reload <object>. This test changes the value of GUC 
log_min_messages to debug. Reloads the cluster and verifies if the change
was reloaded successfully. After the test it resets the value of GUC to 
default.
*/
TEST_F(TestHawqReload,TestReloadLogMinMessages) {
    
    hawq::test::HawqConfig hawq_config;
    string defaultGUCValue = hawq_config.getGucValue("log_min_messages");
    hawq_config.setGucValue("log_min_messages","debug");
    string cmd = "hawq reload cluster -au";
    Command reload(cmd);
    string result = reload.run().getResultOutput();
    EXPECT_EQ("debug", hawq_config.getGucValue("log_min_messages"));
    // Reset to default
    hawq_config.setGucValue("log_min_messages",defaultGUCValue);
    Command setoriginal(cmd);
    string org_result = setoriginal.run().getResultOutput();
}
