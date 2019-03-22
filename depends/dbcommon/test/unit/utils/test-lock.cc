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
#include "dbcommon/thread/thread-base.h"
#include "dbcommon/utils/lock.h"

namespace dbcommon {
#define READTHREADNUM 4
#define WRITETHREADNUM 3

class ReadThread : public dbcommon::ThreadBase {
 public:
  ReadThread() {}
  virtual ~ReadThread() {}

  void init(int32_t id_, int32_t *v, dbcommon::Lock *l, int32_t *t,
            std::mutex *tm, bool uta) {
    id = id_;
    value = v;
    lock = l;
    total = t;
    totalMutex = tm;
    useTimeoutAcquire = uta;
  }

  void run() {
    int32_t times = 10;
    for (int32_t i = 0; i < times; i++) {
      if (useTimeoutAcquire) {
        LockGuard sharedLock(lock, dbcommon::SHAREDLOCK, 1000);
        std::lock_guard<std::mutex> lock(*totalMutex);
        *total += *value;
        LOG_INFO("thread%d: read value=%d, times=%d", id, *value, i);
      } else {
        LockGuard sharedLock(lock, dbcommon::SHAREDLOCK);
        std::lock_guard<std::mutex> lock(*totalMutex);
        *total += *value;
        LOG_INFO("thread%d: read value=%d, times=%d", id, *value, i);
      }
    }
  }

 private:
  int32_t id = 0;
  int32_t *value = nullptr;
  dbcommon::Lock *lock = nullptr;
  int32_t *total = nullptr;
  std::mutex *totalMutex = nullptr;
  bool useTimeoutAcquire = false;
};

class WriteThread : public dbcommon::ThreadBase {
 public:
  WriteThread() {}
  virtual ~WriteThread() {}

  void init(int32_t id_, int32_t *v, dbcommon::Lock *l, int32_t *m, bool uta) {
    id = id_;
    value = v;
    lock = l;
    max = m;
    useTimeoutAcquire = uta;
  }

  void run() {
    int32_t times = 10;
    for (int32_t i = 0; i < times; i++) {
      if (useTimeoutAcquire) {
        LockGuard exclusiveLock(lock, dbcommon::EXCLUSIVELOCK, 1000);
        int32_t v = *value;
        *value = v + id * 100 + i * 10;
        if (*max < *value) *max = *value;
        LOG_INFO("thread%d: write value from %d to %d, times=%d", id, v, *value,
                 i);
      } else {
        LockGuard exclusiveLock(lock, dbcommon::EXCLUSIVELOCK);
        int32_t v = *value;
        *value = v + id * 100 + i * 10;
        if (*max < *value) *max = *value;
        LOG_INFO("thread%d: write value from %d to %d, times=%d", id, v, *value,
                 i);
      }
    }
  }

 private:
  int32_t id = 0;
  int32_t *value = nullptr;
  dbcommon::Lock *lock = nullptr;
  int32_t *max = nullptr;
  bool useTimeoutAcquire = false;
};

TEST(TestLock, TestMultiSharedLocks) {
  bool useTimeoutAcquire[2] = {false, true};
  for (int i = 0; i < 2; i++) {
    LOG_INFO("use timeout acquire = %d", useTimeoutAcquire[i]);
    int32_t value = 233;
    dbcommon::LockManager lockManager;
    std::unique_ptr<Lock> lock = lockManager.create("valueLock");
    ReadThread readThreads[READTHREADNUM];
    int32_t readTotal = 0;
    std::mutex readTotalMutex;

    for (int32_t i = 0; i < READTHREADNUM; i++) {
      readThreads[i].init(i, &value, lock.get(), &readTotal, &readTotalMutex,
                          useTimeoutAcquire[i]);
    }

    for (int32_t i = 0; i < READTHREADNUM; i++) {
      readThreads[i].start();
    }

    for (int32_t i = 0; i < READTHREADNUM; i++) {
      readThreads[i].join();
    }

    EXPECT_EQ(readTotal, value * READTHREADNUM * 10);
  }
}

TEST(TestLock, TestMultiSharedAndExclusiveLocks) {
  bool useTimeoutAcquire[2] = {false, true};
  for (int i = 0; i < 2; i++) {
    LOG_INFO("use timeout acquire = %d", useTimeoutAcquire[i]);
    int32_t value = 233;
    dbcommon::LockManager lockManager;
    std::unique_ptr<Lock> lock = lockManager.create("valueLock");
    ReadThread readThreads[READTHREADNUM];
    WriteThread writeThreads[WRITETHREADNUM];
    int32_t readTotal = 0;
    std::mutex readTotalMutex;
    int32_t writeMax = 0;

    for (int32_t i = 0; i < READTHREADNUM; i++) {
      readThreads[i].init(i, &value, lock.get(), &readTotal, &readTotalMutex,
                          useTimeoutAcquire[i]);
    }
    for (int32_t i = 0; i < WRITETHREADNUM; i++) {
      writeThreads[i].init(i, &value, lock.get(), &writeMax,
                           useTimeoutAcquire[i]);
    }

    for (int32_t i = 0; i < READTHREADNUM; i++) {
      readThreads[i].start();
    }
    for (int32_t i = 0; i < WRITETHREADNUM; i++) {
      writeThreads[i].start();
    }

    for (int32_t i = 0; i < READTHREADNUM; i++) {
      readThreads[i].join();
    }
    for (int32_t i = 0; i < WRITETHREADNUM; i++) {
      writeThreads[i].join();
    }

    EXPECT_GE(writeMax, 4583);
  }
}
}  // namespace dbcommon
