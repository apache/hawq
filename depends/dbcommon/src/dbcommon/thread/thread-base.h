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

//
// Thread.h
//
//  std::thread Thread wrapper:
//  1. inherit from ThreadBase
//  2. implement the virtual function run()
//  3. create your thread and then call start() function
//
//  Example:
//
//  class MyThread: public ThreadBase {
//      MyThread(int mem): member(mem) {}
//      ~MyThread() {}
//
//      void run() {
//          //here is your thread main function;
//      };
//
//    private:
//      int member;
//  }
//
//  MyThread mt;
//  mt.start();
//
//

#ifndef DBCOMMON_SRC_DBCOMMON_THREAD_THREAD_BASE_H_
#define DBCOMMON_SRC_DBCOMMON_THREAD_THREAD_BASE_H_

#include <signal.h>

#include <atomic>
#include <cassert>
#include <functional>
#include <memory>
#include <string>
#include <thread>  //NOLINT
#ifndef __APPLE__
#include <sys/prctl.h>
#endif

#include "dbcommon/log/logger.h"
#include "dbcommon/thread/err-detect-callback.h"

namespace dbcommon {

class ThreadBase {
 public:
  //
  // for each thread-base implemented thread, there is one thread local callback
  // chain, therefore, for all any another thread not using threa-base the
  // callback chain should be explicitly defined
  //
  static thread_local ErrDetectCallbackChain callbackChain;

 public:
  ThreadBase() : requestedThreadExit(false), exited(false) {}
  virtual ~ThreadBase() {}

  virtual void start() {
    thr.reset(new std::thread(&ThreadBase::runFrame, this));
  }

  virtual void stop() {
    requestedThreadExit = true;
    // pthread_kill(this->thr->native_handle(), SIGQUIT);
  }

  void join() {
    if (thr) {
      if (thr->joinable()) {
        thr->join();
      }
    }
  }

  void detach() {
    if (thr) {
      thr->detach();
    }
  }

  bool joinable() {
    if (!thr) return false;

    if (thr->joinable()) {
      return true;
    } else {
      return false;
    }
  }

  void runFrame() {
    if (!thrId.empty()) {
#ifdef __APPLE__
      pthread_setname_np(thrId.c_str());
#else
      prctl(PR_SET_NAME, thrId.c_str());
#endif
    }
    run();
    exited = true;
  }
  virtual void run() {}

  std::thread::id getId() const { return thr->get_id(); }

  void setThreadIdString(const std::string &id) { this->thrId = id; }

  bool threadExitConditionMet() { return requestedThreadExit; }
  bool threadExited() { return exited; }

  static void disableThreadSignals() {
    sigset_t sigs;
    sigemptyset(&sigs);
    // make our thread ignore these signals (which should allow that
    // they be delivered to the main thread)
    sigaddset(&sigs, SIGHUP);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    sigaddset(&sigs, SIGUSR1);
    sigaddset(&sigs, SIGUSR2);
    pthread_sigmask(SIG_BLOCK, &sigs, NULL);
  }

  void prepareCallbackChain(bool withCancelCallback = true);

  static bool shouldReturn() { return callbackChain.call(); }

 protected:
  std::atomic<bool> requestedThreadExit;
  std::atomic<bool> exited;
  std::unique_ptr<std::thread> thr;
  std::string thrId;
};

// this is a pre-defined callback for detecting if the caller's base thread has
// been set cancel required
class ErrDetectCallbackThreadExited : public ErrDetectCallback {
 public:
  explicit ErrDetectCallbackThreadExited(ThreadBase *t)
      : ErrDetectCallback(), thread_(t) {}
  virtual ~ErrDetectCallbackThreadExited() {}
  bool process() override { return thread_->threadExitConditionMet(); }

 protected:
  ThreadBase *thread_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_THREAD_THREAD_BASE_H_
