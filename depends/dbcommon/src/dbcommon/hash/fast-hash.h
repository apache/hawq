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

#ifndef DBCOMMON_SRC_DBCOMMON_HASH_FAST_HASH_H_
#define DBCOMMON_SRC_DBCOMMON_HASH_FAST_HASH_H_

#include <vector>

#include "dbcommon/nodes/select-list.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

class Murmur3 {
 public:
  Murmur3() {}
  virtual ~Murmur3() {}

  static uint64_t hash64(const void* key, uint64_t len) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len >> 3;
    uint64_t hash = kDefaultSeed;

    const uint64_t* blocks = (const uint64_t*)(data);
    for (int i = 0; i < nblocks; i++) {
      uint64_t k = getblock64(blocks, i);
      k *= C1;
      k = rotl64(k, 31);
      k *= C2;
      hash ^= k;
      hash = rotl64(hash, 27) * 5 + 0x52dce729;
    }

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 8);

    uint64_t k1 = 0;

    switch (len & 7) {
      case 7:
        k1 ^= ((uint64_t)tail[6]) << 48;
      case 6:
        k1 ^= ((uint64_t)tail[5]) << 40;
      case 5:
        k1 ^= ((uint64_t)tail[4]) << 32;
      case 4:
        k1 ^= ((uint64_t)tail[3]) << 24;
      case 3:
        k1 ^= ((uint64_t)tail[2]) << 16;
      case 2:
        k1 ^= ((uint64_t)tail[1]) << 8;
      case 1:
        k1 ^= ((uint64_t)tail[0]) << 0;
        k1 *= C1;
        k1 = rotl64(k1, 31);
        k1 *= C2;
        hash ^= k1;
    }

    hash ^= len;
    hash = fmix64(hash);

    return hash;
  }

 private:
  static uint64_t rotl64(uint64_t x, int8_t r) {
    return (x << r) | (x >> (64 - r));
  }

  static uint64_t getblock64(const uint64_t* p, int i) { return p[i]; }

  static uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdLLU;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53LLU;
    k ^= k >> 33;

    return k;
  }

 private:
  static const uint64_t kDefaultSeed = 104729;
  static const uint64_t C1 = 0x87c37b91114253d5LLU;
  static const uint64_t C2 = 0x4cf5ad432745937fLLU;
};

// refer to https://primes.utm.edu/lists/2small/0bit.html
static const uint64_t hashPrimes[] = {
    1ULL,               // 0
    (1ULL << 56) - 5,   // 1
    (1ULL << 48) - 59,  // 2
    1ULL,
    (1ULL << 32) - 5,  // 4
    1ULL,
    1ULL,
    1ULL,
    1ULL,  // 8
};

template <class T, size_t = sizeof(T)>
struct hash_fix_size {
  static void hash(const T* p, uint64_t size,
                   std::vector<uint64_t>& retval) {  // NOLINT
    retval.resize(size);

    for (uint64_t i = 0; i < size; ++i) {
      retval[i] = p[i] * hashPrimes[sizeof(T)];
    }
  }

  static void hash(const T* p, const SelectList& selected,
                   std::vector<uint64_t>& retval) {  // NOLINT
    uint64_t sz = selected.size();
    retval.resize(sz);

    for (uint64_t i = 0; i < sz; ++i) {
      retval[i] = p[selected[i]] * hashPrimes[sizeof(T)];
    }
  }
};

template <typename T>
struct hash_fix_size<T, 8> {
  typedef union {
    struct {
      uint32_t v1;
      uint32_t v2;
    };
    uint64_t v;
  } value_type;

  static void hash(const T* p, uint64_t size,
                   std::vector<uint64_t>& retval) {  // NOLINT
    value_type val;
    retval.resize(size);

    for (size_t i = 0; i < size; ++i) {
      val.v = reinterpret_cast<const uint64_t*>(p)[i];
      retval[i] = val.v;
    }
  }

  static void hash(const T* p, const SelectList& selected,
                   std::vector<uint64_t>& retval) {  // NOLINT
    value_type val;
    uint64_t sz = selected.size();
    retval.resize(sz);

    for (uint64_t i = 0; i < sz; ++i) {
      val.v = reinterpret_cast<const uint64_t*>(p)[selected[i]];
      retval[i] = val.v;
    }
  }
};

template <class T>
struct hash_fix_size<T, 16> {
  typedef union {
    uint64_t v1;
    uint64_t v2;
  } value_type;

  static void hash(const T* p, uint64_t size,
                   std::vector<uint64_t>& retval) {  // NOLINT
    value_type val;
    retval.resize(size);

    for (uint64_t i = 0; i < size; ++i) {
      val.v1 = reinterpret_cast<const uint64_t*>(p)[i * 2];
      val.v2 = reinterpret_cast<const uint64_t*>(p)[i * 2 + 1];
      retval[i] = val.v1 ^ val.v2;
    }
  }

  static void hash(const T* p, const SelectList& selected,
                   std::vector<uint64_t>& retval) {  // NOLINT
    value_type val;
    uint64_t sz = selected.size();
    retval.resize(sz);

    for (uint64_t i = 0; i < sz; ++i) {
      val.v1 = reinterpret_cast<const uint64_t*>(p)[selected[i * 2]];
      val.v2 = reinterpret_cast<const uint64_t*>(p)[selected[i * 2 + 1]];
      retval[i] = val.v1 ^ val.v2;
    }
  }
};

template <typename T>
static inline void fast_hash(const T* p, uint64_t size,
                             std::vector<uint64_t>& retval) {  // NOLINT
  hash_fix_size<T>::hash(p, size, retval);
}

template <typename T>
static inline void fast_hash(const T* p, const SelectList& selected,
                             std::vector<uint64_t>& retval) {  // NOLINT
  hash_fix_size<T>::hash(p, selected, retval);
}

static inline uint64_t BKDR_hash(const char* p, uint64_t size) {
  const char* const end = p + size;
  uint64_t retval = (*p++);
  while (p < end) {
    retval = retval * 131 + (*p++);
  }
  return retval;
}

static inline uint64_t raw_fast_hash(const char* p, uint64_t size) {
  uint64_t res;
  switch (size) {
    case 8:
      res = *reinterpret_cast<const uint64_t*>(p);
      break;
    case 7:
      res = (*reinterpret_cast<const uint32_t*>(p) << 24) +
            (*reinterpret_cast<const uint16_t*>(p) << 8) +
            (*reinterpret_cast<const uint8_t*>(p));
      break;
    case 6:
      res = (*reinterpret_cast<const uint32_t*>(p) << 16) +
            (*reinterpret_cast<const uint16_t*>(p));
      break;
    case 5:
      res = (*reinterpret_cast<const uint32_t*>(p) << 8) +
            (*reinterpret_cast<const uint8_t*>(p));
      break;
    case 4:
      res = *reinterpret_cast<const uint32_t*>(p);
      break;
    case 3:
      res = (*reinterpret_cast<const uint16_t*>(p) << 8) +
            (*reinterpret_cast<const uint8_t*>(p));
      break;
    case 2:
      res = *reinterpret_cast<const uint16_t*>(p);
      break;
    case 1:
      res = *reinterpret_cast<const uint8_t*>(p);
      break;
    default:  // longer than 8 byte
      res = Murmur3::hash64(p, size);
  }

  return res;
}

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_HASH_FAST_HASH_H_
