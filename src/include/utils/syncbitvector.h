/*
 * syncbitvector.h
 *		Interface for synchronized bit vectors.
 *
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
 *
 */

#ifndef SYNCBITVECTOR_H_
#define SYNCBITVECTOR_H_

#define BITVECTOR_SIZE_BINARY_EXP 	(20)
#define BITVECTOR_SIZE_BYTES 		(1L << BITVECTOR_SIZE_BINARY_EXP)	/* 2^20 Bytes = 1MB  */
#define BITVECTOR_SIZE_BITS 		(BITVECTOR_SIZE_BYTES << 3)			/* 2^23 bits */
#define BITVECTOR_OFFSET_MASK 		(BITVECTOR_SIZE_BITS - 1)			/* 0x7FFFFF */

#define SYNC_BITVECTOR_MAX_COUNT 	(100)								/* max total size = 100 MB */

/* bit vector container */
typedef struct SyncBitvectorContainer_s
{
	int64 vectorCount;		// number of vectors stored

	int64 base[0];			// base address for the first vector, 64-bit aligned

} SyncBitvectorContainer_s;

typedef struct SyncBitvectorContainer_s *SyncBitvectorContainer;

/* interface */
extern Size SyncBitVector_ShmemSize(int vectorCount);
extern SyncBitvectorContainer SyncBitVector_ShmemInit(const char *name, int vectorCount);
extern bool SyncBitVector_GetBit(SyncBitvectorContainer container, int32 vectorOffset, int64 bitOffset);
extern void SyncBitVector_SetBit(SyncBitvectorContainer container, int32 vectorOffset, int64 bitOffset);
extern bool SyncBitVector_TestTestSetBit(SyncBitvectorContainer container, int32 vectorOffset, uint64 bitOffset);

#endif /* SYNCBITVECTOR_H_ */
