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
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H
#include <stdint.h>
#include "c.h"

/*
 * Blocked Bloom filter, proposed by FELIX PUTZE et al, in paper
 * Cache-, Hash- and Space-Efficient Bloom Filters.
 * The idea is to divide Bloom filter into several buckets(blocks), each value inserted
 * into the Bloom filter, will be hashed into one bucket. A bucket contains fixed-length bits.
 * This implementation refers to Impala's Bloom filter.
 */
#define NUM_BUCKET_WORDS 8
typedef uint32_t BucketWord;
typedef BucketWord BUCKET[NUM_BUCKET_WORDS];

/* log2(number of bits in a BucketWord) */
#define LOG_BUCKET_WORD_BITS 5

typedef struct BloomFilterData
{
    bool     isCreated;
    uint32_t nBuckets;
    uint32_t nInserted;
    uint32_t nTested;
    uint32_t nMatched;
    size_t   data_size;
    uint32_t data_mask;
    BUCKET   data[1];
} BloomFilterData;
typedef BloomFilterData *BloomFilter;

extern int64_t UpperPowerTwo(int64_t v);
extern BloomFilter InitBloomFilter(int memory_size);
extern void InsertBloomFilter(BloomFilter bf, uint32_t value);
extern bool FindBloomFilter(BloomFilter bf, uint32_t value);
extern void PrintBloomFilter(BloomFilter bf);
extern void DestroyBloomFilter(BloomFilter bf);

#endif
