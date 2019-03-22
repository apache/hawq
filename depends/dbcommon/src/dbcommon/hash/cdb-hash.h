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

#ifndef DBCOMMON_SRC_DBCOMMON_HASH_CDB_HASH_H_
#define DBCOMMON_SRC_DBCOMMON_HASH_CDB_HASH_H_

#include <vector>

#include "dbcommon/log/logger.h"

namespace dbcommon {

#define FNV1_32_INIT ((uint32_t)0x811c9dc5)
/* Constant used for hashing a NULL value */
#define NULL_VAL ((uint32_t)0XF0F0F0F1)

/* Fast mod using a bit mask, assuming that y is a power of 2 */
#define FASTMOD(x, y) ((x) & ((y)-1))

class Fnv1 {
 public:
  Fnv1() {}
  virtual ~Fnv1() {}

  /*
   * fnv1_32_buf - perform a 32 bit FNV 1 hash on a buffer
   *
   * input:
   *	buf - start of buffer to hash
   *	len - length of buffer in octets (bytes)
   *	hval	- previous hash value or FNV1_32_INIT if first call.
   *
   * returns:
   *	32 bit hash as a static hash type
   */
  static uint32_t fnv1_32_buf(const void *buf, size_t len, uint32_t hval) {
    unsigned char *bp = (unsigned char *)buf; /* start of buffer */
    unsigned char *be = bp + len;             /* beyond end of buffer */

    /*
     * FNV-1 hash each octet in the buffer
     */
    while (bp < be) {
/* multiply by the 32 bit FNV magic prime mod 2^32 */
#if defined(NO_FNV_GCC_OPTIMIZATION)
      hval *= FNV_32_PRIME;
#else
      hval +=
          (hval << 1) + (hval << 4) + (hval << 7) + (hval << 8) + (hval << 24);
#endif

      /* xor the bottom with the current octet */
      hval ^= (uint32_t)*bp++;
    }

    /* return our new hash value */
    return hval;
  }
};

static inline uint64_t cdbhash_init() {
  return static_cast<uint64_t>(FNV1_32_INIT);
}

static inline uint64_t cdbhash(const void *buf, size_t len, uint64_t hval) {
  uint32_t retval = Fnv1::fnv1_32_buf(buf, len, static_cast<uint32_t>(hval));
  return static_cast<uint64_t>(retval);
}

static inline uint64_t cdbhashnull(uint64_t hval) {
  uint32_t nullbuf =
      NULL_VAL;  // stores the constant value that represents a NULL
  const void *buf = &nullbuf;    // stores the address of the buffer
  size_t len = sizeof(nullbuf);  // length of the value
  uint32_t retval = Fnv1::fnv1_32_buf(buf, len, static_cast<uint32_t>(hval));
  return static_cast<uint64_t>(retval);
}
}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_HASH_CDB_HASH_H_
