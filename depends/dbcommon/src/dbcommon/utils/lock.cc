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
  std::unique_lock<std::mutex> lock(lockState->stateMutex);
  std::thread::id threadId = std::this_thread::get_id();
  lockState->waiters.push_back(threadId);
  // LOG_INFO(
  //     "Lock::acquire(), before acquire, threadId=%p, mode=%d, "
  //     "lockname is %s",
  //     threadId, mode, lockName.c_str());

  if (mode == EXCLUSIVELOCK) {
    lockState->stateCondition.wait(lock, [this, threadId]() {
      return !lockState->hasExclusive && lockState->sharedCount == 0 &&
             threadId == lockState->waiters.front();
    });
    assert(threadId == lockState->waiters.front());
    assert(!lockState->hasExclusive && lockState->sharedCount == 0);
    lockState->waiters.pop_front();
    lockState->hasExclusive = true;
    assert(lockState->holders.find(threadId) == lockState->holders.end());
    lockState->holders.insert(threadId);
  } else {
    assert(mode == SHAREDLOCK);
    lockState->stateCondition.wait(lock, [this, threadId]() {
      return !lockState->hasExclusive && threadId == lockState->waiters.front();
    });
    assert(threadId == lockState->waiters.front());
    assert(!lockState->hasExclusive);
    lockState->waiters.pop_front();
    if (lockState->holders.find(threadId) != lockState->holders.end()) {
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "One thread can only has one shared lock, "
                "lockName is %s",
                lockName.c_str());
    }
    lockState->sharedCount++;
    lockState->holders.insert(threadId);
  }
}

bool Lock::timeoutAcquire(LockMode mode, int32_t timeout) {
  std::unique_lock<std::mutex> lock(lockState->stateMutex);
  std::thread::id threadId = std::this_thread::get_id();
  lockState->waiters.push_back(threadId);
  // LOG_DEBUG(
  //     "Lock::timeoutAcquire(), before acquire, threadId=%p, mode=%d, "
  //     "timeout=%d",
  //     threadId, mode, timeout);

  if (mode == EXCLUSIVELOCK) {
    if (lockState->stateCondition.wait_for(
            lock, std::chrono::milliseconds(timeout), [this, threadId]() {
              return !lockState->hasExclusive && lockState->sharedCount == 0 &&
                     threadId == lockState->waiters.front();
            })) {
      assert(threadId == lockState->waiters.front());
      assert(!lockState->hasExclusive && lockState->sharedCount == 0);
      lockState->waiters.pop_front();
      lockState->hasExclusive = true;
      assert(lockState->holders.find(threadId) == lockState->holders.end());
      lockState->holders.insert(threadId);
      // LOG_DEBUG(
      //      "Lock::timeoutAcquire(), acquire EXCLUSIVELOCK, threadId=%p, "
      //      "mode=%d,timeout=%d",
      //      threadId, mode, timeout);
      return true;
    } else {
      LOG_INFO("Timeout(%dms) to acquire EXCLUSIVELOCK", timeout);
      for (int32_t i = 0; i < lockState->waiters.size(); i++)
        if (lockState->waiters[i] == threadId)
          lockState->waiters.erase(lockState->waiters.begin() + i);
      return false;
    }
  } else {
    assert(mode == SHAREDLOCK);
    if (lockState->stateCondition.wait_for(
            lock, std::chrono::milliseconds(timeout), [this, threadId]() {
              return !lockState->hasExclusive &&
                     threadId == lockState->waiters.front();
            })) {
      assert(threadId == lockState->waiters.front());
      assert(!lockState->hasExclusive);
      lockState->waiters.pop_front();
      lockState->sharedCount++;
      if (lockState->holders.find(threadId) != lockState->holders.end())
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                  "One thread can only has one shared lock");
      // LOG_DEBUG(
      //      "Lock::timeoutAcquire(), acquire SHAREDLOCK, threadId=%p, "
      //      "mode=%d,timeout=%d",
      //      threadId, mode, timeout);
      lockState->holders.insert(threadId);
      return true;
    } else {
      LOG_INFO("Timeout(%dms) to acquire SHAREDLOCK", timeout);
      for (int32_t i = 0; i < lockState->waiters.size(); i++)
        if (lockState->waiters[i] == threadId)
          lockState->waiters.erase(lockState->waiters.begin() + i);
      return false;
    }
  }
}

void Lock::degrade() {
  std::lock_guard<std::mutex> lock(lockState->stateMutex);
  std::thread::id threadId = std::this_thread::get_id();
  assert(lockState->hasExclusive && lockState->sharedCount == 0);
  lockState->hasExclusive = false;
  lockState->sharedCount++;
  auto it = lockState->holders.find(threadId);
  assert(it != lockState->holders.end());
  lockState->stateCondition.notify_all();
}

bool Lock::isLocked() {
  return lockState->hasExclusive || lockState->sharedCount > 0;
}

bool Lock::isExclusiveLocked() { return lockState->hasExclusive; }

bool Lock::isSharedLocked() { return lockState->sharedCount > 0; }

void Lock::release() {
  std::lock_guard<std::mutex> lock(lockState->stateMutex);
  std::thread::id threadId = std::this_thread::get_id();
  if (lockState->holders.find(threadId) == lockState->holders.end()) return;
  // LOG_INFO(
  //     "Lock::release(), before release, threadId=%p, mode=%d, "
  //     "lockname is %s",
  //     threadId, mode, lockName.c_str());
  lockState->holders.erase(threadId);
  if (lockState->hasExclusive) {
    assert(lockState->sharedCount == 0);
    lockState->hasExclusive = false;
  } else {
    assert(lockState->sharedCount > 0);
    lockState->sharedCount--;
  }
  lockState->stateCondition.notify_all();
}

std::unique_ptr<Lock> LockManager::create(const std::string &lockName) {
  std::lock_guard<std::mutex> lmLock(stateMutex_);
  LockState state;
  state.hasExclusive = false;
  state.sharedCount = 0;
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
      int32_t i = 0;
      for (auto waitId : lockState->waiters) {
        if (waitId == threadId) {
          lockState->waiters.erase(lockState->waiters.begin() + i);
          i--;
        }
        i++;
      }
      continue;
    }
    lockState->holders.erase(threadId);
    if (lockState->hasExclusive) {
      assert(lockState->sharedCount == 0);
      lockState->hasExclusive = false;
    } else {
      assert(lockState->sharedCount > 0);
      lockState->sharedCount--;
    }
    lockState->stateCondition.notify_all();
  }
}

}  // namespace dbcommon
