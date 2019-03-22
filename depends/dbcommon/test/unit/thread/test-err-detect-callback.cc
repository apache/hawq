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

#include "dbcommon/thread/err-detect-callback.h"
#include "dbcommon/thread/thread-base.h"

#include "gtest/gtest.h"

namespace dbcommon {

class MyCallback : public ErrDetectCallback {
 public:
  MyCallback() : ErrDetectCallback(), result_(false) {}
  void setResult(bool r) { result_ = r; }
  bool process() override { return result_; }

 protected:
  volatile bool result_;
};

TEST(TestErrDetectCallback, TestCallback) {
  ErrDetectCallbackChain chain;
  std::unique_ptr<ErrDetectCallback> myCallback(new MyCallback());
  MyCallback *pMyCallback = dynamic_cast<MyCallback *>(myCallback.get());
  chain.addCallback(std::move(myCallback));
  EXPECT_FALSE(chain.call());
  pMyCallback->setResult(true);
  EXPECT_TRUE(chain.call());
  pMyCallback->setResult(false);

  EXPECT_TRUE(chain.call());  // check gotten result
  chain.resetCallResult();    // clear gotten result

  // add second callback, expect false
  std::unique_ptr<ErrDetectCallback> myCallback2(new MyCallback());
  MyCallback *pMyCallback2 = dynamic_cast<MyCallback *>(myCallback2.get());
  chain.addCallback(std::move(myCallback2));
  EXPECT_FALSE(chain.call());

  // let the first one true, expect true
  pMyCallback->setResult(true);
  EXPECT_TRUE(chain.call());

  chain.resetCallResult();  // clear gotten result

  // let the second one true, the first one false, expect true
  pMyCallback->setResult(false);
  pMyCallback2->setResult(true);
  EXPECT_TRUE(chain.call());

  chain.resetCallResult();  // clear gotten result

  // set enable switches to turn off all
  std::vector<bool> switches = {false, false};
  chain.setCallbackSwitches(switches);
  EXPECT_FALSE(chain.call());

  // set enable switches to turn off second one, turn on first one, expect false
  switches = {false, true};
  chain.setCallbackSwitches(switches);
  EXPECT_FALSE(chain.call());

  // set enable switches to turn off first one, turn on second one, expect true
  switches = {true, false};
  chain.setCallbackSwitches(switches);
  EXPECT_TRUE(chain.call());

  chain.resetCallResult();  // clear gotten result

  // clear
  chain.clear();
  EXPECT_EQ(0, chain.getNumOfCallbacks());
}

TEST(TestErrDetectCallback, TestEmptyCallbackChain) {
  ErrDetectCallbackChain chain;
  EXPECT_FALSE(chain.call());
}

TEST(TestErrDetectCallback, TestThreadBaseCancelCallback) {
  ThreadBase thread;
  ErrDetectCallbackChain chain;

  std::unique_ptr<ErrDetectCallback> myCallback(
      new ErrDetectCallbackThreadExited(&thread));
  ErrDetectCallbackThreadExited *pMyCallback =
      dynamic_cast<ErrDetectCallbackThreadExited *>(myCallback.get());
  chain.addCallback(std::move(myCallback));
  EXPECT_FALSE(chain.call());
  thread.stop();
  EXPECT_TRUE(chain.call());
}

TEST(TestErrDetectCallback, TestThreadBasePrepareCallback) {
  ThreadBase thread;
  thread.prepareCallbackChain(false);
  EXPECT_EQ(0, ThreadBase::callbackChain.getNumOfCallbacks());
  thread.prepareCallbackChain(true);
  EXPECT_EQ(1, ThreadBase::callbackChain.getNumOfCallbacks());
  thread.prepareCallbackChain(false);
  EXPECT_EQ(0, ThreadBase::callbackChain.getNumOfCallbacks());
}

}  // namespace dbcommon
