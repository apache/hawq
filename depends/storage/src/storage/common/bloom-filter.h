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

#ifndef STORAGE_SRC_STORAGE_COMMON_BLOOM_FILTER_H_
#define STORAGE_SRC_STORAGE_COMMON_BLOOM_FILTER_H_

#include <algorithm>
#include <cassert>
#include <cmath>

#include "dbcommon/hash/fast-hash.h"

namespace storage {

class MyBitSet {
 public:
  explicit MyBitSet(int64_t bits) : ownData(true) {
    length = static_cast<int64_t>(
        ceil(static_cast<double>(bits) / static_cast<double>(64)));
    data = new uint64_t[length];
    clear();
  }

  MyBitSet(uint64_t *data, int64_t size) : data(data), length(size) {
    assert(data != nullptr && length > 0);
  }

  ~MyBitSet() {
    if (ownData) {
      delete data;
    }
  }

  typedef std::unique_ptr<MyBitSet> uptr;

  void set(int64_t index) {
    assert(index >= 0);
    data[index >> 6] |= (1L << index);
  }

  bool get(int64_t index) {
    assert(index >= 0);
    return (data[index >> 6] & (1L << index)) != 0;
  }

  uint64_t *getData() { return data; }
  int64_t size() { return length; }

  void clear() { memset(data, 0, size() * 8); }

 private:
  uint64_t *data = nullptr;
  int64_t length = 0;
  bool ownData = false;
};

class BloomFilter {
 public:
  explicit BloomFilter(int64_t expectedEntry) : kExpectedEntry(expectedEntry) {
    assert(kExpectedEntry > 0 && "expectedEntries should be > 0");
    assert(kDefaultFpp > 0.0 && kDefaultFpp < 1.0 &&
           "False positive probability should be > 0.0 & < 1.0");
    int64_t nb = optimalNumOfBits(kExpectedEntry, kDefaultFpp);
    numBits = nb + 64 - (nb % 64);
    numHashFunctions = optimalNumOfHashFunctions(kExpectedEntry, numBits);
    bitSet.reset(new MyBitSet(numBits));
  }

  BloomFilter(uint64_t *bits, int64_t size, uint32_t numFuncs) {
    bitSet.reset(new MyBitSet(bits, size));
    numBits = size * 64;
    numHashFunctions = numFuncs;
  }

  virtual ~BloomFilter() {}

  typedef std::unique_ptr<BloomFilter> uptr;

  void addInt(int64_t val) { addHash(getIntegerHash(val)); }
  bool testInt(int64_t val) { return testHash(getIntegerHash(val)); }

  void addDouble(double val) { addInt(doubleToRawBits(val)); }
  bool testDouble(double val) { return testInt(doubleToRawBits(val)); }

  void addString(const char *buffer, uint64_t len) {
    int64_t hash64 = static_cast<int64_t>(murmur3.hash64(buffer, len));
    addHash(hash64);
  }
  bool testString(const char *buffer, uint64_t len) {
    int64_t hash64 = static_cast<int64_t>(murmur3.hash64(buffer, len));
    return testHash(hash64);
  }

  uint64_t *getBitSet() { return bitSet->getData(); }
  int64_t size() { return bitSet->size(); }

  void reset() { bitSet->clear(); }

  uint32_t getNumHashFunctions() { return numHashFunctions; }

 private:
  int64_t optimalNumOfBits(int64_t n, double p) {
    auto ln2 = std::log(2);
    return static_cast<int64_t>(std::ceil(-(n * std::log(p) / ln2 / ln2)));
  }

  uint32_t optimalNumOfHashFunctions(int64_t n, int64_t m) {
    auto frac = static_cast<double>(m) / static_cast<double>(n);
    return static_cast<uint32_t>(std::ceil(frac * std::log(2)));
  }

  int64_t getIntegerHash(int64_t key) {
    key = (~key) + (key << 21);  // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8);  // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4);  // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
  }

  void addHash(int64_t hash64) {
    int64_t hash1 = hash64;
    int64_t hash2 = static_cast<int64_t>(static_cast<uint64_t>(hash64) >> 32);

    for (uint32_t i = 1; i <= numHashFunctions; ++i) {
      int64_t combinedHash = hash1 + (i * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      int64_t pos = combinedHash % numBits;
      bitSet->set(pos);
    }
  }

  bool testHash(int64_t hash64) {
    int64_t hash1 = hash64;
    int64_t hash2 = static_cast<int64_t>(static_cast<uint64_t>(hash64) >> 32);

    for (uint32_t i = 1; i <= numHashFunctions; ++i) {
      int64_t combinedHash = hash1 + (i * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      int64_t pos = combinedHash % numBits;
      if (!bitSet->get(pos)) {
        return false;
      }
    }
    return true;
  }

  int64_t doubleToRawBits(double val) {
    int64_t bits;
    memcpy(&bits, &val, sizeof(bits));
    return bits;
  }

 private:
  int64_t numBits = 0;
  uint32_t numHashFunctions = 0;
  int64_t kExpectedEntry = 0;
  MyBitSet::uptr bitSet = nullptr;
  const double kDefaultFpp = 0.05;
  dbcommon::Murmur3 murmur3;
};

}  // namespace storage

#endif  // STORAGE_SRC_STORAGE_COMMON_BLOOM_FILTER_H_
