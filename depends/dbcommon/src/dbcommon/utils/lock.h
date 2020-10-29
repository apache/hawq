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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_LOCK_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_LOCK_H_

#include <cassert>
#include <condition_variable>  // NOLINT
#include <list>
#include <mutex>  // NOLINT
#include <set>
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>

namespace dbcommon {
enum LockMode { EXCLUSIVELOCK, SHAREDLOCK };

class LockState {
 public:
   bool hasExclusive;
   // <threadId, bool>, when true means this thread wait to acquire exclusive
   // lock and acquire shared lock when false
   std::list<std::pair<std::thread::id, bool>> waiters;
   std::set<std::thread::id> holders;  // one thread only can has one shared lock
   std::mutex stateMutex;
   std::condition_variable stateCondition;

   LockState &operator=(const LockState &L) {
     hasExclusive = L.hasExclusive;
     waiters = L.waiters;
     holders = L.holders;
     return *this;
   }
};

// LockManager lm;
// Lock l = lm.create("abc");
// l.acquire(SHAREDLOCK);

class Lock;

class LockManager {
 public:
  ~LockManager() { assert(locks.size() == 0); }
  std::unique_ptr<Lock> create(const std::string &lockName);
  void drop(const std::string &lockName);
  void releaseThreadAllLocks(const std::thread::id &threadId);

 private:
  std::unordered_map<std::string, LockState> locks;
  std::mutex stateMutex_;
};

class Lock {
 public:
  explicit Lock(const std::string &name) : lockName(name) {}
  ~Lock() {
    release();
    lockMgr->drop(lockName);
  }

  void acquire(LockMode mode);
  bool timeoutAcquire(LockMode mode,
                      int32_t timeout);  // timeout = 0, tryAcquire
  void degrade();                        // degree exclusive lock to shared lock
  bool isLocked();
  bool isExclusiveLocked();
  bool isSharedLocked();
  void release();

  void setLockState(LockState *state) { lockState = state; }
  void setLockManager(LockManager *mgr) { lockMgr = mgr; }

 private:
  bool tryAcquire(LockMode mode);
  void wakeUp();

 private:
  const std::string &lockName;
  LockState *lockState = nullptr;
  LockManager *lockMgr = nullptr;
};

class LockGuard {
 public:
  LockGuard(Lock *lock_, LockMode mode) {
    lock = lock_;
    lock->acquire(mode);
  }

  LockGuard(Lock *lock_, LockMode mode, int32_t timeout) {
    lock = lock_;
    lock->timeoutAcquire(mode, timeout);
  }

  ~LockGuard() { lock->release(); }

 private:
  Lock *lock;
};

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_LOCK_H_
