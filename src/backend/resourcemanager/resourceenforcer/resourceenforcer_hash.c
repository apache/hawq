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

/*
 * resourceenforcer_hash.c
 */
#include "postgres.h"
#include "cdb/cdbvars.h"
#include "access/hash.h"
#include "utils/hsearch.h"
#include "resourcemanager/errorcode.h"
#include "resourceenforcer/resourceenforcer_hash.h"
#include "resourceenforcer/resourceenforcer.h"

/*
 * Functions for getting key hash value.
 */
uint32_t GHash_Hash_UINT32   (void *data);
uint32_t GHash_Hash_VOIDPT   (void *data);
uint32_t GHash_Hash_SIMPSTR  (void *data);
uint32_t GHash_Hash_CHARARRAY(void *data);

/*
 * Functions for comparing keys in GHash.
 */
int32_t GHash_Comp_UINT32   (void *val1, void *val2);
int32_t GHash_Comp_VOIDPT   (void *val1, void *val2);
int32_t GHash_Comp_SIMPSTR  (void *val1, void *val2);
int32_t GHash_Comp_CHARARRAY(void *val1, void *val2);

/*
 * Functions for comparing keys in llist in GHash.
 */
int GHash_List_Comp_UINT32   (void *val1, void *val2);
int GHash_List_Comp_VOIDPT   (void *val1, void *val2);
int GHash_List_Comp_SIMPSTR  (void *val1, void *val2);
int GHash_List_Comp_CHARARRAY(void *val1, void *val2);

/*
 * Functions for freeing key in hash table.
 */
void GHash_KeyFree_UINT32   (void *key);
void GHash_KeyFree_VOIDPT   (void *key);
void GHash_KeyFree_SIMPSTR  (void *key);
void GHash_KeyFree_CHARARRAY(void *key);

/*
 * Functions for copying key of hash table.
 */
void *GHash_KeyCopy_UINT32   (void *key);
void *GHash_KeyCopy_VOIDPT   (void *key);
void *GHash_KeyCopy_SIMPSTR  (void *key);
void *GHash_KeyCopy_CHARARRAY(void *key);

/******************************************************************************
 * Global Variables for GHash implementation.
 *****************************************************************************/
GHashFunctionType _GHashHashFunctions[] =
{
	NULL,
	GHash_Hash_UINT32,
	GHash_Hash_VOIDPT,
	GHash_Hash_SIMPSTR,
	GHash_Hash_CHARARRAY
};

GCompFunctionType _GHashCompFunctions[] =
{
	NULL,
	GHash_Comp_UINT32,
	GHash_Comp_VOIDPT,
	GHash_Comp_SIMPSTR,
	GHash_Comp_CHARARRAY
};

GListCompFunctionType _GHashListCompFunctions[] =
{
	NULL,
	GHash_List_Comp_UINT32,
	GHash_List_Comp_VOIDPT,
	GHash_List_Comp_SIMPSTR,
	GHash_List_Comp_CHARARRAY
};

GKeyFreeFunctionType _GHashKeyFreeFunctions[] =
{
	NULL,
	GHash_KeyFree_UINT32,
	GHash_KeyFree_VOIDPT,
	GHash_KeyFree_SIMPSTR,
	GHash_KeyFree_CHARARRAY
};

GKeyCopyFunctionType _GHashKeyCopyFunctions[] =
{
	NULL,
	GHash_KeyCopy_UINT32,
	GHash_KeyCopy_VOIDPT,
	GHash_KeyCopy_SIMPSTR,
	GHash_KeyCopy_CHARARRAY
};

/******************************************************************************
 * GHash implementation.
 *****************************************************************************/
GHash createGHash(int vol,
                  int maxvol,
                  int keytype,
                  GValFreeFunctionType valfree)
{
	/* Key type argument validity. */
	Assert( keytype > 0 &&
			keytype < sizeof(_GHashHashFunctions) / sizeof(GHashFunctionType) );

	GHash gh = (GHash)malloc( sizeof(struct GHashData) );

	if ( gh == NULL )
	{
		write_log("Function createGHash out of memory");
		return NULL;
	}

	if ( initializeGHash(gh, vol, maxvol, keytype, valfree) != FUNC_RETURN_OK )
	{
		write_log("Function createGHash out of memory during initialization");
		free(gh);
		gh = NULL;
	}

	return gh;
}

int initializeGHash(GHash table,
                    int vol,
                    int maxvol,
                    int keytype,
                    GValFreeFunctionType valfree)
{
	Assert( table );
	/* Key type argument validity. */
	Assert( keytype > 0 &&
			keytype < sizeof(_GHashHashFunctions) / sizeof(GHashFunctionType) );

	table->HashFunction		= _GHashHashFunctions[keytype];
	table->CompFunction		= _GHashCompFunctions[keytype];
	table->ListCompFunction	= _GHashListCompFunctions[keytype];
	table->KeyFreeFunction	= _GHashKeyFreeFunctions[keytype];
	table->KeyCopyFunction	= _GHashKeyCopyFunctions[keytype];
	table->ValFreeFunction	= valfree;
	table->KeyType			= keytype;
	table->NodeCount		= 0;
	table->SlotCount		= 0;
	table->SlotVolume		= vol;
	table->SlotVolumeMax	= maxvol;
	table->Slots			= (llist **)malloc(sizeof(llist *) * vol);
	table->SlotExtendBar	= 0.5;

	if ( table->Slots == NULL )
	{
		write_log("Function initializeGHash out of memory");
		return UTIL_HASHTABLE_OUT_OF_MEMORY;
	}

	for ( int i = 0; i < table->SlotVolume; ++i )
	{
		table->Slots[i] = NULL;
	}

	return FUNC_RETURN_OK;
}

/******************************************************************************
 * Set the value of specified key.
 *
 * If the key does not exist, new (key,value) is inserted into the hash table.
 * If the key exists, the value is updated, and the old value is returned.
 * If the table is full and can not be extended, error is returned.
 *****************************************************************************/
int setGHashNode(GHash table, void *key, void *value, bool freeOld, void **oldValue)
{
	Assert( table );
	Assert( table->HashFunction != NULL );
	Assert( !freeOld || (freeOld && table->ValFreeFunction != NULL) );
	Assert( oldValue );

	uint32_t	hashvalue	= 0;
	uint32_t	slot		= 0;
	Pair		newpair		= NULL;
	void		*retvalue	= NULL;
	lnode		*node		= NULL;
	int			res			= FUNC_RETURN_OK;

	/* Get key hash value. */
	hashvalue = table->HashFunction(key);
	slot	  = hashvalue % table->SlotVolume;

	/* Add new node to the slot. The slot maybe NULL. */
	if ( table->Slots[slot] == NULL )
	{
		table->Slots[slot] = llist_create();

		if (table->Slots[slot] == NULL)
		{
			write_log("Function setGHashNode out of memory during list creation");
			return UTIL_HASHTABLE_OUT_OF_MEMORY;
		}

		table->SlotCount++;
	}
	else
	{
		llist_foreach(node, table->Slots[slot])
		{
			Pair pair = (Pair)(llist_lfirst(node));
			if ( table->CompFunction(key, pair->Key) == 0 )
			{
				newpair = pair;
				break;
			}
		}
	}

	/* Insert new pair with copied key object, the value object is assigned. */
	if ( newpair == NULL )
	{
		newpair = createPair(NULL, value);

		if ( newpair == NULL )
		{
			write_log("Function setGHashNode out of memory during pair creation");
			return UTIL_HASHTABLE_OUT_OF_MEMORY;
		}

		newpair->Key = (void *)(table->KeyCopyFunction(key));

		res = llist_insert(table->Slots[slot], (void *)newpair);

		if ( res != 0 )
		{
			/* Free key object */
			if ( newpair->Key != NULL )
			{
				table->KeyFreeFunction(newpair->Key);
			}

			/* Free pair object */
			free(newpair);

			write_log("Function setGHashNode out of memory");
			return UTIL_HASHTABLE_OUT_OF_MEMORY;
		}

		table->NodeCount++;
	}
	else
	{
		retvalue = newpair->Value;
		newpair->Value = value;
	}

	/* Double the size of the hash table if slot volume should be extended. */
	if ( (table->SlotCount / table->SlotVolume > table->SlotExtendBar) &&
		 (table->SlotVolume < table->SlotVolumeMax) )
	{
		int newhashsize = table->SlotVolume * 2 > table->SlotVolumeMax ?
						  table->SlotVolumeMax : table->SlotVolume * 2 ;

		res = extendGHashSlotSize(table, newhashsize);

		if ( res != FUNC_RETURN_OK )
		{
			write_log("Function setGHashNode out of memory during extendGHashSlotSize");
			return UTIL_HASHTABLE_OUT_OF_MEMORY;
		}
	}

	/* Free value if should do it. */
	if ( freeOld )
	{
		if ( retvalue != NULL )
		{
			table->ValFreeFunction(retvalue);
			retvalue = NULL;
		}
		*oldValue = NULL;
	}
	else
	{
		*oldValue = retvalue;
	}

	return FUNC_RETURN_OK;
}

Pair getGHashNode(GHash table, void *key)
{
	Assert( table );

	uint32_t	hashvalue	= 0;
	uint32_t	slot		= 0;
	lnode		*node		= NULL;

	/* Get key hash value */
	hashvalue = table->HashFunction(key);
	slot	  = hashvalue % table->SlotVolume;

	if ( table->Slots[slot] == NULL )
	{
		return NULL;	/* No such key. */
	}

	llist_foreach(node, table->Slots[slot])
	{
		Pair pair = (Pair)(llist_lfirst(node));

		if ( table->CompFunction(key, pair->Key) == 0 )
		{
			return pair;
		}
	}

	return NULL;	/* No such key. */
}

int removeGHashNode(GHash table, void *key)
{
	Assert( table );
	Assert( key );

	uint32_t	hashvalue	= 0;
	uint32_t	slot		= 0;

	/* Get key hash value */
	hashvalue = table->HashFunction(key);
	slot	  = hashvalue % table->SlotVolume;

	if ( llist_size(table->Slots[slot]) == 0 )
	{
		return UTIL_HASHTABLE_NOKEY; /* No such key. */
	}

	/* Search the slot and get the pair to be deleted from the list */
	lnode * delnode = llist_delete(table->Slots[slot], key, table->ListCompFunction);

	if ( delnode == NULL )
	{
		return UTIL_HASHTABLE_NOKEY;
	}

	if ( llist_size(table->Slots[slot]) == 0 )
	{
		table->SlotCount--;
	}

	table->NodeCount--;

	Pair delpair = (Pair)(llist_lfirst(delnode));
	/* Free key object */
	if ( delpair->Key != NULL )
	{
		table->KeyFreeFunction(delpair->Key);
	}

	/* Free value object */
	if ( table->ValFreeFunction != NULL )
	{
		table->ValFreeFunction(delpair->Value);
	}

	/* Free pair object */
	freePair(delpair);

	return FUNC_RETURN_OK;
}

void clearGHashSlots(GHash table, llist **slots, int slotssize)
{
	Assert( table );
	Assert( slots );
	Assert( slotssize > 0 );

	lnode	*pairnode	= NULL;

	for ( int i = 0; i < slotssize; ++i )
	{
		if (slots[i] == NULL)
		{
			continue;
		}

		llist_foreach(pairnode, slots[i])
		{
			Pair pair = (Pair)llist_lfirst(pairnode);

			if ( pair->Key != NULL )
			{
				table->KeyFreeFunction(pair->Key);
			}

			if ( table->ValFreeFunction != NULL )
			{
				table->ValFreeFunction(pair->Value);
			}

			freePair(pair);
		}

		llist_destroy(slots[i]);
		slots[i] = NULL;
	}
}

void clearGHash(GHash table)
{
	Assert( table );

	lnode	*pairnode	= NULL;

	for ( int i = 0; i < table->SlotVolume; ++i )
	{
		llist_foreach(pairnode, table->Slots[i])
		{
			Pair pair = (Pair)llist_lfirst(pairnode);

			if ( pair->Key != NULL )
			{
				table->KeyFreeFunction(pair->Key);
			}

			if ( table->ValFreeFunction != NULL )
			{
				table->ValFreeFunction(pair->Value);
			}

			freePair(pair);
		}

		llist_destroy(table->Slots[i]);
		table->Slots[i] = NULL;
	}

	table->NodeCount = 0;
	table->SlotCount = 0;
}

void cleanGHash(GHash table)
{
	Assert( table );

	clearGHash( table );	/* Clear all hash table nodes. */
	free(table->Slots);
	table->Slots = NULL;
}

void freeGHash(GHash table)
{
	Assert( table );
	cleanGHash( table );
	free(table);
}

uint32_t getGHashSize(GHash table)
{
	Assert( table );
	return table->NodeCount;
}

uint32_t getGHashVolume(GHash table)
{
	Assert( table );
	return table->SlotVolume;
}

uint32_t GHash_Hash_UINT32(void *data)
{
	return *((uint32_t *)data);
}

uint32_t GHash_Hash_VOIDPT(void *data)
{
	return *((uint32_t *)(&data));
}

uint32_t GHash_Hash_SIMPSTR(void *data)
{
	uint32_t res = string_hash(((GSimpStringPtr)data)->Str,
							   ((GSimpStringPtr)data)->Len);
	return res;
}

uint32_t GHash_Hash_CHARARRAY(void *data)
{
	return DatumGetUInt32(
				hash_any((const unsigned char *)((GSimpArrayPtr)data)->Array,
						 (int)((GSimpArrayPtr)data)->Len));
}

int32_t GHash_Comp_UINT32 (void *val1, void *val2)
{
	return (*((uint32_t *)val1)) - (*((uint32_t *)val2));
}

int32_t GHash_Comp_VOIDPT (void *val1, void *val2)
{
	return (*((uint32_t *)(&val1))) - (*((uint32_t *)(&val2)));
}

int32_t GHash_Comp_SIMPSTR(void *val1, void *val2)
{
	return strcmp(((GSimpString *)val1)->Str, ((GSimpString *)val2)->Str);
}

int32_t GHash_Comp_CHARARRAY(void *val1, void *val2)
{
	return GSimpArrayComp((GSimpArrayPtr)val1, (GSimpArrayPtr)val2);
}

int GHash_List_Comp_UINT32(void *val1, void *val2)
{
	Assert( val1 );
	Assert( val2 );

	uint32_t lval = *((uint32_t *)(((Pair)(val1))->Key));
	uint32_t rval = *((uint32_t *)val2);

	return lval - rval;
}

int GHash_List_Comp_VOIDPT(void *val1, void *val2)
{
	Assert( val1 );
	Assert( val2 );

	uint32_t lval = *((uint32_t *)(&(((Pair)(val1))->Key)));
	uint32_t rval = *((uint32_t *)(&val2));

	return lval - rval;
}

int GHash_List_Comp_SIMPSTR(void *val1, void *val2)
{
	Assert( val1 );
	Assert( val2 );

	GSimpStringPtr lval = (GSimpStringPtr)(((Pair)(val1))->Key);
	GSimpStringPtr rval = (GSimpStringPtr)val2;

	return strcmp(lval->Str, rval->Str);
}

int GHash_List_Comp_CHARARRAY(void *val1, void *val2)
{
	Assert( val1 );
	Assert( val2 );

	GSimpArrayPtr lval = (GSimpArrayPtr)(((Pair)(val1))->Key);
	GSimpArrayPtr rval = (GSimpArrayPtr)val2;

	return GSimpArrayComp(lval, rval);
}

void GHash_KeyFree_UINT32(void *key)
{
	/* Do nothing. */
}

void GHash_KeyFree_VOIDPT(void *key)
{
	/* Do nothing. */
}

void GHash_KeyFree_SIMPSTR(void *key)
{
	freeGSimpStringContent((GSimpStringPtr)key);
	free(key);
}

void GHash_KeyFree_CHARARRAY(void *key)
{
	freeGSimpArrayContent((GSimpArrayPtr)key);
	free(key);
}

void *GHash_KeyCopy_UINT32(void *key)
{
	return (void *)key;
}

void *GHash_KeyCopy_VOIDPT(void *key)
{
	return (void *)key;
}

void *GHash_KeyCopy_SIMPSTR(void *key)
{
	Assert( key );

	GSimpString *gss = createGSimpString();

	if ( gss == NULL )
	{
		write_log("Function GHash_KeyCopy_SIMPSTR out of memory");
		return NULL;
	}

	initGSimpStringWithContent( gss,
								((GSimpString *)key)->Str,
								((GSimpString *)key)->Len);
	return gss;
}

void *GHash_KeyCopy_CHARARRAY(void *key)
{
	Assert( key );

	GSimpArrayPtr gsap = createGSimpArray();

	if ( gsap == NULL )
	{
		write_log("Function GHash_KeyCopy_CHARARRAY out of memory");
		return NULL;
	}

	setGSimpArrayWithContent( gsap,
							  ((GSimpArrayPtr)key)->Array,
							  ((GSimpArrayPtr)key)->Len);
	return gsap;
}

void getAllPairRefIntoList(GHash table, llist **list)
{
	Assert( table );
	Assert( list );

	lnode *pairnode = NULL;

	for ( int i = 0 ; i < table->SlotVolume ; ++i )
	{
		llist_foreach(pairnode, table->Slots[i])
		{
			llist_insert(*list, llist_lfirst(pairnode));
		}
	}
}

void freePairRefList(GHash table, llist **list)
{
	Assert( table );
	Assert( list );

	llist_destroy(*list);
	*list = NULL;
}

int extendGHashSlotSize(GHash table, int newsize)
{
	Assert( table );
	Assert( newsize > table->SlotVolume );

	lnode *pairnode = NULL;

	llist **newslot = (llist **)malloc(sizeof(llist *) * newsize);

	if ( newslot == NULL )
	{
		write_log("Function extendGHashSlotSize out of memory");
		return UTIL_HASHTABLE_OUT_OF_MEMORY;
	}

	for ( int i = 0; i < newsize; ++i )
	{
		newslot[i]= NULL;
	}

	table->SlotCount = 0;
	for ( int i = 0; i < table->SlotVolume; ++i )
	{
		llist_foreach(pairnode, table->Slots[i])
		{
			Pair pair = (Pair)llist_lfirst(pairnode);
			uint32_t hashvalue = table->HashFunction(pair->Key);
			int newindex = hashvalue % newsize;

			if ( newslot[newindex] == NULL )
			{
				newslot[newindex] = llist_create();

				if ( newslot[newindex] == NULL )
				{
					write_log("Function extendGHashSlotSize out of memory during list creation");
					clearGHashSlots(table, newslot, newsize);
					free(newslot);
					return UTIL_HASHTABLE_OUT_OF_MEMORY;
				}

				table->SlotCount++;
			}

			if ( llist_insert(newslot[newindex], pair) != FUNC_RETURN_OK )
			{
				write_log("Function extendGHashSlotSize out of memory during re-hashing");
				clearGHashSlots(table, newslot, newsize);
				free(newslot);
				return UTIL_HASHTABLE_OUT_OF_MEMORY;
			}
		}

		llist_destroy(table->Slots[i]);
	}

	free(table->Slots);
	table->Slots = newslot;
	table->SlotVolume = newsize;

	return FUNC_RETURN_OK;
}

void dumpGHash(GHash table)
{
	Assert( table );

	write_log("###### Dumping Generic Hash Content - Begins ######");
	write_log("######");
	write_log("###### Slot Volume     = %d", table->SlotVolume);
	write_log("###### Slot Volume Max = %d", table->SlotVolumeMax);
	write_log("###### Slots Count     = %d", table->SlotCount);
	write_log("###### Nodes Count     = %d", table->NodeCount);
	write_log("######");

	for ( int i = 0 ; i < table->SlotVolume ; ++i )
	{
		if ( (table->Slots[i] == NULL) || (llist_size(table->Slots[i]) == 0) )
		{
			continue;
		}

		write_log("###### Slot %d: %d Nodes", i, table->Slots[i] == NULL ? 0 : llist_size(table->Slots[i]));
		lnode *no = (table->Slots[i])->head;
		while (no)
		{
			Pair pair = (Pair)(no->data);
			GSimpStringPtr str = (GSimpStringPtr)(pair->Key);
			CGroupInfo *cgi = (CGroupInfo *)(pair->Value);

			write_log("######    CGroup = %s   Name          = %s", str->Str, cgi->name);
			write_log("######    CGroup = %s   Creation Time = "UINT64_FORMAT"", str->Str, cgi->creation_time);
			write_log("######    CGroup = %s   VCore Current = %d", str->Str, cgi->vcore_current);
			write_log("######    CGroup = %s   Vdisk Current = %d", str->Str, cgi->vdisk_current);
			write_log("######    CGroup = %s   To Be Deleted = %d", str->Str, cgi->to_be_deleted);
			write_log("######    CGroup = %s   # PIDS        = %d", str->Str, cgi->pids->size);
			lnode *ni = (cgi->pids->head);
			while (ni)
			{
				write_log("######    CGroup = %s   PID           = %d", str->Str, (*(int *)(ni->data)));
				ni = ni->next;
			}

			no = no->next;
		}
	}

	write_log("######");
	write_log("###### Dumping Generic Hash Content - Ends ######");
}

