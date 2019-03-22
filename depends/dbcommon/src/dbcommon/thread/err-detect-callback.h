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

#ifndef DBCOMMON_SRC_DBCOMMON_THREAD_ERR_DETECT_CALLBACK_H_
#define DBCOMMON_SRC_DBCOMMON_THREAD_ERR_DETECT_CALLBACK_H_

#include <cassert>
#include <list>
#include <memory>
#include <vector>

//
// a chained callback facility to help make decision if a retry in synchronized
// call should continue. The motivation of introducing this into the thread
// framework is : when a thread calls a method which internally will retry until
// something is done or some error is encoutered, this thread has no chance to
// notify that method to stop retrying
//
// this facility allows the caller to prepare a chain of callbacks to perform
// different checking logics and when there is at least one callback finding an
// error the result is set as true.
//
//   +------------------------+   1:N    +-------------------+
//   | ErrDetectCallbackChain |--------->| ErrDetectCallback |
//   +------------------------+          +-------------------+
//
// ErrDetectCallbackChain provides management interface to help build up one
// chain consumed by the callback caller
namespace dbcommon {

class ErrDetectCallback {
 public:
  ErrDetectCallback() : next_(nullptr), enabled_(true) {}
  virtual ~ErrDetectCallback() {}
  virtual bool process() = 0;
  // main entry to judge if some error is detected, this is not thread-safe
  // interface, we dont expect this is used by more than one thread
  // simultaneously
  bool call() {
    if (enabled_) {
      if (process()) {
        return true;
      }
    }
    if (next_ != nullptr) {
      return next_->call();
    }
    return false;
  }

  void setNext(ErrDetectCallback *n) { next_ = n; }
  void setEnabled(bool newVal) { enabled_ = newVal; }

 protected:
  bool enabled_;  // only enabled callback works actually, a disabled one always
                  // return false
  // NOTE: we dont expect many callback instances are saved in one chain, maybe
  //       at most 3 callbacks. Thus, we can safely use recursive implementation
  ErrDetectCallback *next_;  // short cut to next callback to build up recursive
                             // process
};

class ErrDetectCallbackChain {
 public:
  ErrDetectCallbackChain() : gotCallResult_(false) {}

  void clear() {
    callbacks_.clear();
    gotCallResult_ = false;
  }

  void addCallback(std::unique_ptr<ErrDetectCallback> callback) {
    ErrDetectCallback *next = nullptr;
    if (callbacks_.size() > 0) {
      next = callbacks_.front().get();
    }
    callback->setNext(next);
    callbacks_.push_front(std::move(callback));
  }

  bool call() {
    if (callbacks_.size() == 0) {
      return false;  // no callback instance to call, return false
    }
    if (gotCallResult_) {
      return gotCallResult_;  // callbacks are not allowed to be called twice
                              // when true result was gotten
    }
    gotCallResult_ = callbacks_.front()->call();
    return gotCallResult_;
  }

  void setCallbackSwitches(const std::vector<bool> &switches) {
    // the vector size should be equal to existing callbacks
    assert(switches.size() == callbacks_.size());

    auto callbackIter = callbacks_.begin();
    auto switchIter = switches.begin();

    for (; switchIter != switches.end() && callbackIter != callbacks_.end();
         ++callbackIter, ++switchIter) {
      (*callbackIter)->setEnabled(*switchIter);
    }
  }

  uint32_t getNumOfCallbacks() const { return callbacks_.size(); }
  bool checkCallResult() const { return gotCallResult_; }
  void resetCallResult() { gotCallResult_ = false; }

 protected:
  std::list<std::unique_ptr<ErrDetectCallback>> callbacks_;
  bool gotCallResult_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_THREAD_ERR_DETECT_CALLBACK_H_
