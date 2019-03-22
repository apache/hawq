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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_ASYNC_QUEUE_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_ASYNC_QUEUE_H_

#include <algorithm>
#include <cassert>
#include <condition_variable>  //NOLINT
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>  //NOLINT
#include <queue>
#include <thread>  //NOLINT
#include <unordered_map>

#include "dbcommon/log/logger.h"

namespace dbcommon {

class AsyncQueuePushCallback {
 public:
  virtual ~AsyncQueuePushCallback() {}

  typedef std::unique_ptr<AsyncQueuePushCallback> uptr;
  typedef std::unordered_map<uint64_t, std::unique_ptr<AsyncQueuePushCallback>>
      uptrmap;

  virtual void* callback() = 0;
};

class AsyncQueueBase {
 public:
  AsyncQueueBase() {}
  virtual ~AsyncQueueBase() {}

  void registerCallbackForPush(uint64_t tag, AsyncQueuePushCallback::uptr cb) {
    std::lock_guard<std::mutex> lock(callbackMutex);
    assert(callbackForPush.find(tag) == callbackForPush.end());
    callbackForPush[tag] = std::move(cb);
  }

  void unregisterCallbackForPush(uint64_t tag) {
    std::lock_guard<std::mutex> lock(callbackMutex);
    assert(callbackForPush.find(tag) != callbackForPush.end());
    callbackForPush.erase(tag);
  }

  void triggerCallbacks() {
    std::lock_guard<std::mutex> guard(callbackMutex);
    for (auto ci = callbackForPush.begin(); ci != callbackForPush.end(); ++ci) {
      ci->second->callback();
    }
  }

 protected:
  std::mutex callbackMutex;
  // registered (optional) callback to notify someone interested in this push
  // action
  AsyncQueuePushCallback::uptrmap callbackForPush;
};

template <class T, class Container = std::list<std::unique_ptr<T>>>
class AsyncQueue : public AsyncQueueBase {
  typedef typename Container::value_type value_type;
  typedef typename Container::size_type size_type;
  typedef typename Container::iterator iterator_type;
  typedef Container container_type;

 public:
  AsyncQueue() {}
  // The move copy constructor
  AsyncQueue(AsyncQueue&& sq) {  // NOLINT
    maxNumItems = sq.maxNumItems;
    elemQueue = std::move(sq.elemQueue);
  }
  AsyncQueue(const AsyncQueue& sq) = delete;
  // The copy assignment operator
  AsyncQueue& operator=(const AsyncQueue& aq) = delete;
  // The move assignment operator
  AsyncQueue& operator=(AsyncQueue&& aq) = delete;

  virtual ~AsyncQueue() {}

  // Sets the maximum number of items in the queue.
  // Defaults is 0: No limit
  // @param num  The maximal number of items set in the queue
  // @return Void
  void setMaxNumItems(unsigned int num) { maxNumItems = num; }

  // Pushes the item into the queue.
  // @param item The item to push to the queue
  // @return Void
  void push(value_type item) {
    std::unique_lock<std::mutex> lock(queueMutex);
    if (maxNumItems > 0) {
      producerCondition.wait(lock, [this]() {  // Lambda funct
        return elemQueue.size() < maxNumItems;
      });
    }
    elemQueue.push_back(std::move(item));
    consumerCondition.notify_one();

    numItemsPush++;

    assert(numItemsPush == elemQueue.size() + this->numItemsPop);

    triggerCallbacks();
  }

  // Pushes the item into the queue.
  // Note: please do not use timeoutPush(timeout, std::move(item)),
  //       where item is of type "value_type", use timeoutPush(timeout, item);
  // here we use "value_type &" for parameter item. Since if the timeoutPush
  // failed, we do not want to change the value of the input parameter.
  //
  // @param timeout  The timeout value
  // @param item  The item to push to the queue
  // @return false If timeout, otherwise true
  bool timeoutPush(std::uint64_t timeout, value_type& item) {
    std::unique_lock<std::mutex> lock(queueMutex);
    if (maxNumItems > 0) {
      if (!producerCondition.wait_for(lock, std::chrono::milliseconds(timeout),
                                      [this]() {  // Lambda funct
                                        return elemQueue.size() < maxNumItems;
                                      })) {
        return false;  // timeout
      }
    }

    elemQueue.push_back(std::move(item));
    consumerCondition.notify_one();

    numItemsPush++;

    assert(numItemsPush == elemQueue.size() + this->numItemsPop);

    triggerCallbacks();

    return true;
  }

  // Return pointer to the front element of the queue, caller should guarantee
  // the value part is not released after working on the return value as queue
  // does not guarantee to keep this element after calling.
  // @return pointer to the first element's value
  T* frontPtr() {
    std::unique_lock<std::mutex> lock(queueMutex);
    return elemQueue.size() == 0 ? nullptr : elemQueue.front().get();
  }

  // Pushes the item into the front of the queue.
  // @param item The item to push
  // @return Void
  void pushFront(value_type item) {
    std::unique_lock<std::mutex> lock(queueMutex);

    if (maxNumItems > 0) {
      producerCondition.wait(lock, [this]() {  // Lambda funct
        return elemQueue.size() < maxNumItems;
      });
    }

    elemQueue.push_front(std::move(item));
    consumerCondition.notify_one();

    numItemsPush++;

    assert(numItemsPush == elemQueue.size() + this->numItemsPop);

    triggerCallbacks();
  }

  // Pops item from the queue.
  // If queue is empty, this function blocks until item becomes available.
  void pop(value_type& item) {  // NOLINT
    std::unique_lock<std::mutex> lock(queueMutex);

    consumerCondition.wait(lock, [this]() {  // Lambda funct
      return !elemQueue.empty();
    });

    item = std::move(elemQueue.front());
    elemQueue.pop_front();
    producerCondition.notify_one();

    numItemsPop++;

    assert(numItemsPush == elemQueue.size() + this->numItemsPop);
  }

  // Try to pop item from the queue.
  // return "False" if no item is available.
  bool tryPop(value_type& item) {  // NOLINT
    std::unique_lock<std::mutex> lock(queueMutex);

    if (elemQueue.empty()) return false;

    assert(elemQueue.size() > 0);

    item = std::move(elemQueue.front());
    elemQueue.pop_front();
    producerCondition.notify_one();

    numItemsPop++;

    if (numItemsPush != (elemQueue.size() + this->numItemsPop)) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "numItemsPush %llu numItemsPop %llu queueSz %lu", numItemsPush,
                numItemsPop, elemQueue.size());
    }

    return true;
  }

  // Wait until the queue is not empty. If the queue is empty, blocks for
  // timeout
  // @param timeout  The timeout value
  // @return True if the queue is not empty, otherwise false
  //  NOTE: we cannot guarantee that it is still not empty when you check it
  //  after
  //  this call returns.
  bool timeoutWait(std::uint64_t timeout) {
    std::unique_lock<std::mutex> lock(queueMutex);

    if (consumerCondition.wait_for(lock, std::chrono::milliseconds(timeout),
                                   [this]() { return !elemQueue.empty(); })) {
      // got an item
      assert(!elemQueue.empty());
      return true;
    } else {
      // timeout
      return false;
    }
  }

  // Pops item from the queue. If the queue is empty, blocks for timeout
  // milliseconds, or until item becomes available.
  // timeout - milliseconds to wait.
  // return true if get an item from the queue, false if no item is received
  // before the timeout.
  bool timeoutPop(std::uint64_t timeout, value_type& item) {  // NOLINT
    std::unique_lock<std::mutex> lock(queueMutex);

    if (consumerCondition.wait_for(lock, std::chrono::milliseconds(timeout),
                                   [this]() { return !elemQueue.empty(); })) {
      // got an item
      assert(!elemQueue.empty());
      item = std::move(elemQueue.front());

      elemQueue.pop_front();
      producerCondition.notify_one();

      numItemsPop++;

      assert(numItemsPush == elemQueue.size() + this->numItemsPop);
      return true;
    } else {
      assert(numItemsPush == elemQueue.size() + this->numItemsPop);
      // timeout
      return false;
    }
  }

  // Gets the number of items in the queue.
  int size() {
    std::lock_guard<std::mutex> lock(queueMutex);

    assert(numItemsPush == elemQueue.size() + this->numItemsPop);
    return elemQueue.size();
  }

  // Check if the queue is empty.
  // return true if queue is empty.
  bool empty() const {
    std::lock_guard<std::mutex> lock(queueMutex);

    assert(numItemsPush == elemQueue.size() + this->numItemsPop);
    return elemQueue.empty();
  }

  // Swaps the contents.
  // aq - The AsyncQueue to swap with 'this'.
  void swap(AsyncQueue& aq) {
    if (this != &aq) {
      std::lock_guard<std::mutex> lock1(queueMutex);
      std::lock_guard<std::mutex> lock2(aq.queueMutex);
      elemQueue.swap(aq.elemQueue);
      maxNumItems = aq.maxNumItems;

      if (!elemQueue.empty()) consumerCondition.notify_all();
      if (!aq.elemQueue.empty()) aq.consumerCondition.notify_all();

      if (elemQueue.size() < maxNumItems) producerCondition.notify_all();
      if (aq.elemQueue.size() < aq.maxNumItems)
        aq.producerCondition.notify_all();
    }
  }

  void clear() {
    std::lock_guard<std::mutex> lock(queueMutex);
    elemQueue.clear();

    numItemsPush = 0;
    numItemsPop = 0;
  }

 private:
  std::mutex queueMutex;
  std::deque<std::unique_ptr<T> /*, Container*/> elemQueue;
  std::condition_variable consumerCondition;
  std::condition_variable producerCondition;
  unsigned int maxNumItems = 0;

  // statistics
  uint64_t numItemsPush = 0;
  uint64_t numItemsPop = 0;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_ASYNC_QUEUE_H_
