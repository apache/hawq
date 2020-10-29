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

#include <cassert>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/lock.h"

namespace dbcommon {

void Lock::acquire(LockMode mode) {
  if (tryAcquire(mode)) return;

  std::thread::id threadId = std::this_thread::get_id();
  std::unique_lock<std::mutex> lock(lockState->stateMutex);
  lockState->stateCondition.wait(lock, [this, threadId]() {
    return lockState->holders.find(threadId) != lockState->holders.end();
  });
}

bool Lock::timeoutAcquire(LockMode mode, int32_t timeout) {
  if (tryAcquire(mode)) return true;

  std::thread::id threadId = std::this_thread::get_id();
  std::unique_lock<std::mutex> lock(lockState->stateMutex);
  if (lockState->stateCondition.wait_for(
          lock, std::chrono::milliseconds(timeout), [this, threadId]() {
            return lockState->holders.find(threadId) !=
                   lockState->holders.end();
          })) {
    return true;
  } else {
    LOG_DEBUG("Timeout(%dms) to acquire lock: %s, threadId = %p", timeout,
              lockName.c_str(), threadId);
    for (auto waitThread = lockState->waiters.begin();
         waitThread != lockState->waiters.end(); waitThread++)
      if (waitThread->first == threadId) lockState->waiters.erase(waitThread);
    return false;
  }
}

void Lock::degrade() {
  std::lock_guard<std::mutex> lock(lockState->stateMutex);
  std::thread::id threadId = std::this_thread::get_id();
  assert(lockState->hasExclusive && lockState->holders.size() == 1);
  lockState->hasExclusive = false;
  auto it = lockState->holders.find(threadId);
  assert(it != lockState->holders.end());
  wakeUp();
}

bool Lock::isLocked() { return lockState->holders.size() > 0; }

bool Lock::isExclusiveLocked() { return lockState->hasExclusive; }

bool Lock::isSharedLocked() {
  return !lockState->hasExclusive && lockState->holders.size() > 0;
}

void Lock::release() {
  std::thread::id threadId = std::this_thread::get_id();
  std::lock_guard<std::mutex> lock(lockState->stateMutex);
  if (lockState->holders.find(threadId) == lockState->holders.end()) return;
  lockState->holders.erase(threadId);
  if (lockState->hasExclusive) {
    lockState->hasExclusive = false;
  }
  assert(!lockState->hasExclusive);

  wakeUp();
}

bool Lock::tryAcquire(LockMode mode) {
  std::thread::id threadId = std::this_thread::get_id();
  std::lock_guard<std::mutex> lock(lockState->stateMutex);
  if (mode == EXCLUSIVELOCK) {
    if (lockState->holders.size() == 0) {
      assert(!lockState->hasExclusive);
      lockState->hasExclusive = true;
      lockState->holders.insert(threadId);
      return true;
    }
    lockState->waiters.push_back(std::make_pair(threadId, true));
  } else {
    if (!lockState->hasExclusive) {
      lockState->holders.insert(threadId);
      return true;
    }
    lockState->waiters.push_back(std::make_pair(threadId, false));
  }
  return false;
}

// lockState->stateMutex is already held by caller
void Lock::wakeUp() {
  if (lockState->waiters.size() == 0) return;

  // first waiting thread can get this lock
  if (lockState->holders.size() == 0) {
    auto waitThread = lockState->waiters.front();
    if (waitThread.second) {
      lockState->hasExclusive = true;
    }
    lockState->holders.insert(waitThread.first);
    lockState->waiters.pop_front();
  }

  // if this lock is shared lock, other threads acquiring shared lock can get
  // this lock
  if (!lockState->hasExclusive) {
    for (auto waitThread = lockState->waiters.begin();
         waitThread != lockState->waiters.end(); waitThread++) {
      if (!waitThread->second) {
        // shared lock can be granted
        lockState->holders.insert(waitThread->first);
        lockState->waiters.erase(waitThread);
      }
    }
  }
  lockState->stateCondition.notify_all();
}

std::unique_ptr<Lock> LockManager::create(const std::string &lockName) {
  std::lock_guard<std::mutex> lmLock(stateMutex_);
  LockState state;
  state.hasExclusive = false;
  state.waiters.clear();
  state.holders.clear();
  assert(locks.find(lockName) == locks.end());
  locks[lockName] = state;
  std::thread::id threadId = std::this_thread::get_id();
  // LOG_DEBUG("LockManager::create(), threadId=%p", threadId);
  auto it = locks.find(lockName);
  assert(it != locks.end());
  std::unique_ptr<Lock> lock(new Lock(it->first));
  lock->setLockState(&(it->second));
  lock->setLockManager(this);
  return std::move(lock);
}

void LockManager::drop(const std::string &lockName) {
  std::lock_guard<std::mutex> lmLock(stateMutex_);
  if (locks.find(lockName) == locks.end()) return;
  locks.erase(lockName);
}

void LockManager::releaseThreadAllLocks(const std::thread::id &threadId) {
  for (auto lockIt = locks.begin(); lockIt != locks.end(); lockIt++) {
    LockState *lockState = &lockIt->second;
    std::lock_guard<std::mutex> lock(lockState->stateMutex);
    std::thread::id threadId = std::this_thread::get_id();
    if (lockState->holders.find(threadId) == lockState->holders.end()) {
      for (auto waitThread = lockState->waiters.begin();
           waitThread != lockState->waiters.end(); waitThread++) {
        if (waitThread->first == threadId) {
          lockState->waiters.erase(waitThread);
        }
      }
      continue;
    }
    lockState->holders.erase(threadId);
    lockState->stateCondition.notify_all();
  }
}

}  // namespace dbcommon
