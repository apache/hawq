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

/*--------------------------------------------------------------------------
*
* cdbhash.h
*	 Definitions and API functions for cdbhash.c
*
*
*--------------------------------------------------------------------------
*/
#ifndef CDBHASH_H
#define CDBHASH_H

/*
 * hashing algorithms.
 */
typedef enum
{
	HASH_FNV_1 = 1,
	HASH_FNV_1A
} CdbHashAlg;

/*
 * reduction methods.
 */
typedef enum
{
	REDUCE_LAZYMOD = 1,
	REDUCE_BITMASK
} CdbHashReduce;

typedef uint32 CdbHashFn (void *, size_t, uint32);

/*
 * Structure that holds Greenplum Database hashing information.
 */
typedef struct CdbHash
{
	uint32		hash;			/* The result hash value							*/
	int			numsegs;		/* number of segments in Greenplum Database used for
								 * partitioning  */
	CdbHashAlg	hashalg;		/* the hashing algorithm							*/
	CdbHashFn  *hashfn;			/* hashing function for the selected hash
								 * algorithm */
	CdbHashReduce reducealg;	/* the algorithm used for reducing to buckets		*/
	uint32		rrindex;		/* round robin index for empty policy tables		*/

} CdbHash;


typedef void (*datumHashFunction)(void *clientData, void *buf, size_t len);

extern void hashDatum(Datum datum, Oid type, datumHashFunction hashFn, void *clientData);
extern void hashNullDatum(datumHashFunction hashFn, void *clientData);

/*
 * Create and initialize a CdbHash in the current memory context.
 * Parameter numsegs - number of segments in Greenplum Database.
 * Parameter algorithm - the hash algorithm, either HASH_FNV_1 or HASH_FNV_1A
 */
extern CdbHash *makeCdbHash(int numsegs, CdbHashAlg algorithm);

/*
 * Initialize CdbHash for hashing the next tuple values.
 */
extern void cdbhashinit(CdbHash *h);

/*
 * Add an attribute to the hash calculation.
 */
extern void cdbhash(CdbHash *h, Datum val, Oid typid);

/*
 * Add a NULL attribute to the hash calculation.
 */
extern void cdbhashnull(CdbHash *h);

/*
 * Hash a tuple for a relation with an empty (no hash keys) partitioning policy.
 */
extern void cdbhashnokey(CdbHash *h);

/*
 * Reduce the hash to a segment number.
 */
extern unsigned int cdbhashreduce(CdbHash *h);

/*
 * Return true if Oid is hashable internally in Greenplum Database.
 */
extern bool isGreenplumDbHashable(Oid typid);

/*
 * Return true if the Oid is an array type.  This can be used prior
 *   to hashing the datum because array typeoids are expected to
 *   have been converted to any array oid.
 */
extern bool typeIsArrayType(Oid typeoid);

#endif   /* CDBHASH_H */
