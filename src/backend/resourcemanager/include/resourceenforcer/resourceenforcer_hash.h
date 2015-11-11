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

#ifndef _HAWQ_RESOURCEENFORCER_HASH_H
#define _HAWQ_RESOURCEENFORCER_HASH_H

#include "resourceenforcer_pair.h"
#include "resourceenforcer_simpstring.h"
#include "resourceenforcer_list.h"

// #include "resourceenforcer/resourceenforcer_pair.h"
// #include "resourceenforcer/resourceenforcer_simpstring.h"
// #include "resourceenforcer/resourceenforcer_list.h"
/******************************************************************************
 * A simple version of chained hash table for generic purpose.
 *
 * This hash table is built as one array of double direction link lists. All
 * keys having the same hash value are saved in one list as one slot(bucket).
 * Dynamic hash table slot extension is supported, and the iteration of the hash
 * table is also supported.
 *
 * The key object is always copied as one copied object in hashtable's context,
 * when new key is added. The value object is not copied, but user can decide
 * if the value object can be freed along with the free / clear of hash table.
 * This is defined by the variable valfree in
 *
 * createHASHTABLE / initializeHASHTABLE.
 *
 * Currently, only 3 key types are supported :
 *
 * unsigned 32-bit integer		HASH_TABLE_KEYTYPE_UINT32
 * void pointer					HASH_TABLE_KEYTYPE_VOIDPT
 * simple string object			HASH_TABLE_KEYTYPE_SIMPSTR
 *
 * NOTE: To improve: 	1) iteration of (key,values);
 *	 	 	 	 		2) BST replaces DQueue(double direction link list);
 *  	 	 	 		3) User-defined key type and functions.
 *
 *                                                    .
 *           --------------------                    / \
 *           |  HASHTABLE       |                   /   \
 *           | (value,HASHNODE) |--ref BST node -> / BST \
 *           --------------------                 /_______\
 *
 *          * Hash table helps to              * Log time complexity to
 *            quickly position BST               get ordered values in a
 *            node by value in BST               range.
 *
 ******************************************************************************/
#define GHASH_KEYTYPE_UINT32		1
#define GHASH_KEYTYPE_VOIDPT		2
#define GHASH_KEYTYPE_SIMPSTR		3
#define GHASH_KEYTYPE_CHARARRAY		4

/* Hashing and comparison function types. */
typedef uint32_t (* GHashFunctionType) (void *);
typedef int32_t  (* GCompFunctionType) (void *, void *);
typedef int  (* GListCompFunctionType) (void *, void *);
typedef void  (* GKeyFreeFunctionType) (void *);
typedef void *(* GKeyCopyFunctionType) (void *);
typedef void  (* GValFreeFunctionType) (void *);

/* Default Hashtable volumes */
#define GHASH_SLOT_VOLUME_DEFAULT		256
#define GHASH_SLOT_VOLUME_DEFAULT_MAX	8192

/* Hash table structure. */
struct GHashData
{
	GHashFunctionType		HashFunction;		/* Hash value function     */
	GCompFunctionType		CompFunction;		/* Compare function        */
	GListCompFunctionType	ListCompFunction;	/* List compare function   */
	GKeyFreeFunctionType	KeyFreeFunction;	/* Free function for key   */
	GKeyCopyFunctionType	KeyCopyFunction;	/* Copy function for key   */
	GValFreeFunctionType	ValFreeFunction;	/* Free function for value */

	llist					**Slots;			/* Slots for all values    */

	int						KeyType;
	uint32_t				NodeCount;
	uint32_t				SlotCount;
	uint32_t				SlotVolume;
	uint32_t				SlotVolumeMax;

	/* [0.5,1] double value, if SlotCount/SlotVolume > SlotExtendBar, the array
 	 * of slots are automatically doubled until SlotVolumeMax is achieved. If 1
 	 * is set, the hash table slot number is not automatically extended. */
	double					SlotExtendBar;
};

typedef struct GHashData *GHash;
typedef struct GHashData GHashData;

#define UTIL_HASHTABLE_NOKEY         1
#define UTIL_HASHTABLE_OUT_OF_MEMORY 2

GHash createGHash(int vol,
                  int maxval,
                  int keytype,
                  GValFreeFunctionType valfree);

int initializeGHash(GHash table,
                    int vol,
                    int maxval,
                    int keytype,
                    GValFreeFunctionType valfree);

int setGHashNode(GHash table,
                 void *key,
                 void *value,
                 bool freeOld,
                 void **oldValue);

Pair getGHashNode(GHash table, void *key);
int removeGHashNode(GHash table, void *key);
void clearGHashSlots(GHash table, llist **slots, int slotssize);
void clearGHash(GHash table);
void cleanGHash(GHash table);
void freeGHash(GHash table);
uint32_t getGHashSize(GHash table);
uint32_t getGHashVolume(GHash table);

int extendGHashSlotSize(GHash table, int newsize);
void getAllPairRefIntoList(GHash table, llist **list);
void freePairRefList(GHash table, llist **list);

void dumpGHash(GHash table);

#endif /* _HAWQ_RESOURCEENFORCER_HASH_H */
