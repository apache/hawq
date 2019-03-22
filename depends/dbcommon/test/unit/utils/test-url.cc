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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "gtest/gtest.h"

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/url.h"

void bad(const std::string &path) {
  using dbcommon::URL;

  URL url(path);
}

TEST(TestURL, TestBadFileSystemURL1) {
  using dbcommon::URL;

  std::string path1 = "dfs:/localhost";
  EXPECT_THROW(bad(path1), dbcommon::TransactionAbortException);

  std::string path2 = "dfs://";
  EXPECT_THROW(bad(path2), dbcommon::TransactionAbortException);

  std::string path3 = "://localhost";
  EXPECT_THROW(bad(path3), dbcommon::TransactionAbortException);

  std::string path4 = "hdfs://localhost:";
  EXPECT_THROW(bad(path3), dbcommon::TransactionAbortException);
}

TEST(TestURL, TestFileSystemURL) {
  using dbcommon::URL;

  URL url1("hdfs://localhost:9000");

  EXPECT_EQ(url1.getProtocol(), "hdfs");
  EXPECT_EQ(url1.getNormalizedServiceName(), "hdfs://localhost:9000/");
  EXPECT_EQ(url1.getPath(), "/");
  EXPECT_EQ(url1.getQuery(), "");
  EXPECT_EQ(url1.getHost(), "localhost");
  EXPECT_EQ(url1.getPort(), 9000);

  URL url2("hdfs://localhost:9000/");

  EXPECT_EQ(url2.getProtocol(), "hdfs");
  EXPECT_EQ(url2.getNormalizedServiceName(), "hdfs://localhost:9000/");
  EXPECT_EQ(url2.getPath(), "/");
  EXPECT_EQ(url2.getQuery(), "");
  EXPECT_EQ(url2.getHost(), "localhost");
  EXPECT_EQ(url2.getPort(), 9000);

  URL url3("file://localhost/cn");

  EXPECT_EQ(url3.getProtocol(), "file");
  EXPECT_EQ(url3.getNormalizedServiceName(), "file://localhost/");
  EXPECT_EQ(url3.getPath(), "/cn");
  EXPECT_EQ(url3.getQuery(), "");
  EXPECT_EQ(url3.getHost(), "localhost");
  EXPECT_EQ(url3.getPort(), URL::EMPTY_PORT);

  URL url4("file:///cn");

  EXPECT_EQ(url4.getProtocol(), "file");
  EXPECT_EQ(url4.getNormalizedServiceName(), "file:///");
  EXPECT_EQ(url4.getPath(), "/cn");
  EXPECT_EQ(url4.getQuery(), "");
  EXPECT_EQ(url4.getHost(), "");
  EXPECT_EQ(url4.getPort(), URL::INVALID_PORT);

  URL url5("hdFS://localhost:9000/cn");

  EXPECT_EQ(url5.getProtocol(), "hdfs");
  EXPECT_EQ(url5.getNormalizedServiceName(), "hdfs://localhost:9000/");
  EXPECT_EQ(url5.getPath(), "/cn");
  EXPECT_EQ(url5.getQuery(), "");
  EXPECT_EQ(url5.getHost(), "localhost");
  EXPECT_EQ(url5.getPort(), 9000);

  URL url6("hdFS://localhost:9000/cn?a=3");

  EXPECT_EQ(url6.getProtocol(), "hdfs");
  EXPECT_EQ(url6.getNormalizedServiceName(), "hdfs://localhost:9000/");
  EXPECT_EQ(url6.getPath(), "/cn");
  EXPECT_EQ(url6.getQuery(), "a=3");
  EXPECT_EQ(url6.getHost(), "localhost");
  EXPECT_EQ(url6.getPort(), 9000);

  URL url7("file:///");

  EXPECT_EQ(url7.getProtocol(), "file");
  EXPECT_EQ(url7.getNormalizedServiceName(), "file:///");
  EXPECT_EQ(url7.getPath(), "/");
  EXPECT_EQ(url7.getQuery(), "");
  EXPECT_EQ(url7.getHost(), "");
  EXPECT_EQ(url7.getPort(), URL::INVALID_PORT);
}
