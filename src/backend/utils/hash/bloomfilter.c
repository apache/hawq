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

#include "utils/bloomfilter.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "lib/stringinfo.h"
#include <assert.h>

const static uint32_t HASH_SEEDS[8] = { 0x14EBCDFFU,
        0x2A1C1A99U, 0x85CB78FBU, 0x6E8F82DDU, 0xF8464DFFU, 0x1028FEADU,
        0x74F04A4DU, 0x1832DB75U };

static uint32_t getBucketIdx(uint32_t hash, uint32_t mask)
{
    /* use Knuth's multiplicative hash */
    return ((uint64_t) (hash) * 2654435769ul >> 32) & mask;
}

/*
 * Returns the smallest power of two that is bigger than v.
 */
int64_t UpperPowerTwo(int64_t v) {
    --v;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    ++v;
    return v;
}

/*
 * Initialize a Bloom filter structure with the memory size of Bloom filter.
 */
BloomFilter InitBloomFilter(int memory_size)
{
    BloomFilter bf;
    uint32_t nBuckets = max(1, memory_size/(sizeof(BucketWord)*NUM_BUCKET_WORDS));
    size_t size = nBuckets*NUM_BUCKET_WORDS*sizeof(BucketWord);
    bf = palloc0(offsetof(BloomFilterData, data)  + size);
    bf->nInserted = bf->nTested = bf->nMatched = 0;
    bf->nBuckets = nBuckets;
    bf->data_mask = bf->nBuckets - 1;
    bf->data_size = size;
    bf->isCreated = true;
    elog(DEBUG3, "Create a Bloom filter with number of buckets:%d, size:%d",
                 bf->nBuckets, size);
    return bf;
}

/*
 * Insert a value into Bloom filter.
 */
void InsertBloomFilter(BloomFilter bf, uint32_t value)
{
    uint32_t bucket_idx = getBucketIdx(value, bf->data_mask);
    uint32_t new_bucket[8];
    for (int i = 0; i < NUM_BUCKET_WORDS; ++i) {
        /*
         * Multiply-shift hashing proposed by Dietzfelbinger et al.
         * hash a value universally into 5 bits using the random odd seed.
         */
        new_bucket[i] = (HASH_SEEDS[i] * value) >> (32 - LOG_BUCKET_WORD_BITS);
        new_bucket[i] = 1U << new_bucket[i];
        bf->data[bucket_idx][i] |= new_bucket[i];
    }
}

/*
 * Check whether a value is in this Bloom filter or not.
 */
bool FindBloomFilter(BloomFilter bf, uint32_t value)
{
    bf->nTested++;
    uint32_t bucket_idx = getBucketIdx(value, bf->data_mask);
    for (int i = 0; i < NUM_BUCKET_WORDS; ++i)
    {
        BucketWord hval = (HASH_SEEDS[i] * value) >> (32 - LOG_BUCKET_WORD_BITS);
        hval = 1U << hval;
        if (!(bf->data[bucket_idx][i] & hval))
        {
            return false;
        }
    }
    bf->nMatched++;
    return true;
}

void PrintBloomFilter(BloomFilter bf)
{
    StringInfo bfinfo = makeStringInfo();
    appendStringInfo(bfinfo, "##### Print Bloom filter #####\n");
    appendStringInfo(bfinfo, "data_mask: %x\n ", bf->data_mask);
    appendStringInfo(bfinfo, "number of buckets: %d\n ", bf->nBuckets);
    appendStringInfo(bfinfo, "number of inserted: %d\n ", bf->nInserted);
    appendStringInfo(bfinfo, "number of tested: %d\n ", bf->nTested);
    appendStringInfo(bfinfo, "number of matched: %d\n ", bf->nMatched);
    appendStringInfo(bfinfo, "size: %d\n ", bf->data_size);
    appendStringInfo(bfinfo, "##### END Print Bloom filter #####\n");
    elog(LOG, "%s", bfinfo->data);
    if(bfinfo->data != NULL)
    {
        pfree(bfinfo->data);
    }
}

/*
 * Destroy a Bloom filter structure.
 */
void DestroyBloomFilter(BloomFilter bf)
{
    if (bf != NULL)
    {
        pfree(bf);
    }
    elog(DEBUG3, "Destroy a Bloom filter");
}
