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

#ifndef _HASH_TABLE_H
#define _HASH_TABLE_H
#include "resourcemanager/envswitch.h"

#include "resourcemanager/utils/pair.h"
#include "resourcemanager/utils/linkedlist.h"
#include "utils/hsearch.h"

/******************************************************************************
 * A simple version of chained hash table.
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
#define HASHTABLE_KEYTYPE_UINT32		1
#define HASHTABLE_KEYTYPE_VOIDPT		2
#define HASHTABLE_KEYTYPE_SIMPSTR		3
#define HASHTABLE_KEYTYPE_CHARARRAY		4

/* Hashing and comparison function types. */
typedef uint32_t (* HashFunctionType) (void *);
typedef uint32_t (* CompFunctionType) (void *, void *);
typedef void  (* KeyFreeFunctionType) (MCTYPE, void *);
typedef void *(* KeyCopyFunctionType) (MCTYPE, void *);
typedef void  (* ValFreeFunctionType) (void *);

/* Default Hashtable volumes */
#define HASHTABLE_SLOT_VOLUME_DEFAULT			256
#define HASHTABLE_SLOT_VOLUME_DEFAULT_MAX		1024*1024*1024

/* Hash table structure. */
struct HASHTABLEData {

#ifdef BUILT_IN_HAWQ
	MemoryContext			Context;			/* Memory context        	 */
#else
    void           		   *Context;
#endif

    HashFunctionType		HashFunction;		/* Hash value function   	 */
    CompFunctionType		CompFunction;		/* Compare function      	 */
    KeyFreeFunctionType		KeyFreeFunction;	/* Free function for key 	 */
    KeyCopyFunctionType		KeyCopyFunction;	/* Copy function for key 	 */
    ValFreeFunctionType		ValFreeFunction;	/* Free function for value   */

    List				  **Slots;				/* Slots for all values. 	 */

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

typedef struct HASHTABLEData *HASHTABLE;
typedef struct HASHTABLEData HASHTABLEData;

#define UTIL_HASHTABLE_NOKEY 1

HASHTABLE createHASHTABLE ( MCTYPE 		  		context,
							int 		  		vol,
							int 		  		maxval,
							int 		  		keytype,
							ValFreeFunctionType valfree);

void initializeHASHTABLE  ( HASHTABLE	  		table,
							MCTYPE 				context,
						    int 		 	 	vol,
						    int 		  		maxval,
						    int 		  		keytype,
						    ValFreeFunctionType valfree);

void *setHASHTABLENode ( HASHTABLE   table,
					     void 		*key,
					     void 		*value,
					     bool   	 freeOld);

PAIR	  getHASHTABLENode	  ( HASHTABLE table, void *key );
int	  	  removeHASHTABLENode ( HASHTABLE table, void *key );
void	  clearHASHTABLE	  ( HASHTABLE table );
void	  cleanHASHTABLE	  ( HASHTABLE table );
/*
void	  freeHASHTABLE		  ( HASHTABLE table );
*/
void	  extendHASHTABLESlotSize ( HASHTABLE table, int newsize );
void	  getAllPAIRRefIntoList   ( HASHTABLE table, List **list );
void 	  freePAIRRefList		  ( HASHTABLE table, List **list );
#endif /* _HASH_TABLE_H */
