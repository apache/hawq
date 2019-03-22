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

#include "dbcommon/utils/async-queue.h"

namespace dbcommon {

class QueueItem {
 public:
  QueueItem(std::string name) : name(name) {}

 private:
  std::string name;
};

TEST(TestAsyncQueue, TestBasic) {
  AsyncQueue<QueueItem> queue;

  std::string val = "i1";

  std::unique_ptr<QueueItem> inItem(new QueueItem("i1"));
  queue.push(std::move(inItem));

  std::unique_ptr<QueueItem> outItem;
  queue.pop(outItem);

  EXPECT_STREQ(val.c_str(), outItem->name.c_str());
}

TEST(TestAsyncQueue, TestTimeout) {
  AsyncQueue<QueueItem> queue;
  std::string val1 = "i1";
  std::string val2 = "i2";

  std::unique_ptr<QueueItem> outItem;
  bool ret = queue.timeoutPop(10, outItem);
  EXPECT_EQ(ret, false);

  ret = queue.tryPop(outItem);
  EXPECT_EQ(ret, false);

  queue.setMaxNumItems(1);
  std::unique_ptr<QueueItem> inItem1(new QueueItem(val1));
  queue.push(std::move(inItem1));

  std::unique_ptr<QueueItem> inItem2(new QueueItem(val2));
  ret = queue.timeoutPush(10, inItem2);
  EXPECT_EQ(ret, false);

  queue.setMaxNumItems(2);
  ret = queue.timeoutPush(10, inItem2);
  EXPECT_EQ(ret, true);
  EXPECT_EQ(inItem2.get(), nullptr);

  ret = queue.tryPop(outItem);
  EXPECT_EQ(ret, true);
  EXPECT_STREQ(val1.c_str(), outItem->name.c_str());
  EXPECT_EQ(queue.size(), 1);

  queue.pop(outItem);
  EXPECT_STREQ(val2.c_str(), outItem->name.c_str());
}

}  // namespace dbcommon
