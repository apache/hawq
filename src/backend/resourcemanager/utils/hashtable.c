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

#include "utils/hashtable.h"
#include "utils/simplestring.h"

/*
 * Functions for getting key hash value.
 */
uint32_t HASHTABLE_Hash_UINT32   (void *data);
uint32_t HASHTABLE_Hash_VOIDPT   (void *data);
uint32_t HASHTABLE_Hash_SIMPSTR  (void *data);
uint32_t HASHTABLE_Hash_CHARARRAY(void *data);

/*
 * Functions for comparing keys.
 */
uint32_t HASHTABLE_Comp_UINT32   (void *val1, void *val2);
uint32_t HASHTABLE_Comp_VOIDPT   (void *val1, void *val2);
uint32_t HASHTABLE_Comp_SIMPSTR  (void *val1, void *val2);
uint32_t HASHTABLE_Comp_CHARARRAY(void *val1, void *val2);

/*
 * NOTE: Key free functions run under the memory context of hash table.
 */
void HASHTABLE_KeyFree_UINT32   (MCTYPE context, void *key);
void HASHTABLE_KeyFree_VOIDPT   (MCTYPE context, void *key);
void HASHTABLE_KeyFree_SIMPSTR  (MCTYPE context, void *key);
void HASHTABLE_KeyFree_CHARARRAY(MCTYPE context, void *key);

/*
 * NOTE: Key copy functions run under the memory context of hash table.
 */
void *HASHTABLE_KeyCopy_UINT32   (MCTYPE context, void *key);
void *HASHTABLE_KeyCopy_VOIDPT   (MCTYPE context, void *key);
void *HASHTABLE_KeyCopy_SIMPSTR  (MCTYPE context, void *key);
void *HASHTABLE_KeyCopy_CHARARRAY(MCTYPE context, void *key);

/******************************************************************************
 * Global Variables for HASHTABLE implementation.
 *****************************************************************************/
HashFunctionType _HASHTABLEHashFunctions[] = {
	NULL,
	HASHTABLE_Hash_UINT32,
	HASHTABLE_Hash_VOIDPT,
	HASHTABLE_Hash_SIMPSTR,
	HASHTABLE_Hash_CHARARRAY
};

CompFunctionType _HASHTABLECompFunctions[] = {
	NULL,
	HASHTABLE_Comp_UINT32,
	HASHTABLE_Comp_VOIDPT,
	HASHTABLE_Comp_SIMPSTR,
	HASHTABLE_Comp_CHARARRAY
};

KeyFreeFunctionType _HASHTABLEKeyFreeFunctions[] = {
	NULL,
	HASHTABLE_KeyFree_UINT32,
	HASHTABLE_KeyFree_VOIDPT,
	HASHTABLE_KeyFree_SIMPSTR,
	HASHTABLE_KeyFree_CHARARRAY
};

KeyCopyFunctionType _HASHTABLEKeyCopyFunctions[] = {
	NULL,
	HASHTABLE_KeyCopy_UINT32,
	HASHTABLE_KeyCopy_VOIDPT,
	HASHTABLE_KeyCopy_SIMPSTR,
	HASHTABLE_KeyCopy_CHARARRAY
};

/******************************************************************************
 * HASHTABLE implementation.
 *****************************************************************************/
HASHTABLE createHASHTABLE ( MCTYPE 				context,
							int 		  		vol,
							int 		  		maxvol,
							int 		  		keytype,
							ValFreeFunctionType valfree)
{
    HASHTABLE res = NULL;

    /* Key type argument validity. */
    Assert( keytype > 0 &&
    		keytype < sizeof(_HASHTABLEHashFunctions) /
    				  sizeof(HashFunctionType));

    res = (HASHTABLE) rm_palloc0 ( context,
    									 sizeof(struct HASHTABLEData) );
    initializeHASHTABLE( res, context, vol, maxvol, keytype, valfree);
    return res;
}

void initializeHASHTABLE  ( HASHTABLE	  table,
							MCTYPE 		  context,
						    int 		  vol,
						    int 		  maxvol,
						    int 		  keytype,
						    ValFreeFunctionType valfree)
{
	Assert( table != NULL );
    /* Key type argument validity. */
    Assert( keytype > 0 &&
    		keytype < sizeof(_HASHTABLEHashFunctions) /
    				  sizeof(HashFunctionType));

    table->Context 			= context;
    table->HashFunction 	= _HASHTABLEHashFunctions[keytype];
    table->CompFunction		= _HASHTABLECompFunctions[keytype];
    table->KeyFreeFunction  = _HASHTABLEKeyFreeFunctions[keytype];
    table->KeyCopyFunction  = _HASHTABLEKeyCopyFunctions[keytype];
    table->ValFreeFunction	= valfree;
    table->KeyType			= keytype;
    table->NodeCount 		= 0;
    table->SlotCount		= 0;
    table->SlotVolume		= vol;
    table->SlotVolumeMax	= maxvol;
    table->Slots			= (List **)rm_palloc0(context, sizeof(List *) * vol);
    table->SlotExtendBar	= 0.5;

    for ( int i = 0 ; i < table->SlotVolume ; ++i ) {
    	table->Slots[i] = NULL;
    }

}

/******************************************************************************
 * Set the value of specified key.
 *
 * If the key does not exist, new (key,value) is inserted into the hash table.
 * If the key exists, the value is updated, and the old value is returned.
 * If the table is full and can not be extended, error is returned.
 *****************************************************************************/
void *setHASHTABLENode( HASHTABLE    table,
					    void 		*key,
					    void 		*value,
					    bool 		 freeOld)
{
	uint32_t  hashvalue = 0;
	uint32_t  slot 		= 0;
	PAIR	  newpair	= NULL;
	void     *res		= NULL;
	ListCell *paircell  = NULL;

	Assert(table != NULL);
	Assert(table->HashFunction != NULL);
	Assert(!freeOld || (freeOld && table->ValFreeFunction != NULL));

	/* Get key hash value. */
	hashvalue = table->HashFunction(key);
	slot 	  = hashvalue % table->SlotVolume;


	/* Add new node to the slot. The slot maybe NULL. */
	if ( table->Slots[slot] == NULL )
	{
		table->SlotCount++;
	}
	else
	{
		foreach(paircell, table->Slots[slot])
		{
			PAIR pair = (PAIR)(lfirst(paircell));
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
		newpair = createPAIR(table->Context, NULL, value);
		newpair->Key = (void *)(table->KeyCopyFunction(table->Context, key));
		MEMORY_CONTEXT_SWITCH_TO(table->Context)
		table->Slots[slot] = lappend(table->Slots[slot], (void *)newpair);
		MEMORY_CONTEXT_SWITCH_BACK
		table->NodeCount++;
	}
	else
	{
		res = newpair->Value;
		newpair->Value = value;
	}

	/* Double the size of the hash table if slot volume should be extended.  */
	if ( table->SlotCount/table->SlotVolume > table->SlotExtendBar &&
		 table->SlotVolume < table->SlotVolumeMax ) {
		extendHASHTABLESlotSize( table,
								 table->SlotVolume * 2 > table->SlotVolumeMax ?
								 table->SlotVolumeMax :
								 table->SlotVolume *2 );
	}

	/* Free value if should do it. */
	if ( res != NULL && freeOld ) {
    	table->ValFreeFunction(res);
    	res = NULL;
	}

	return res;
}

PAIR getHASHTABLENode( HASHTABLE table, void *key )
{
	uint32_t  hashvalue  = 0;
	uint32_t  slot 		 = 0;
	ListCell *paircell   = NULL;
	Assert(table != NULL);

	// Get key hash value
	hashvalue = table->HashFunction(key);
	slot 	  = hashvalue % table->SlotVolume;

	foreach(paircell, table->Slots[slot])
	{
		PAIR curnode = (PAIR)(lfirst(paircell));
		if ( table->CompFunction(curnode->Key, key) == 0 )
		{
			return curnode;
		}
	}
	return NULL;	/* No such key. */
}

int	removeHASHTABLENode( HASHTABLE table, void *key )
{
	uint32_t  hashvalue = 0;
	uint32_t  slot 		= 0;
	ListCell *paircell  = NULL;
	ListCell *prevcell  = NULL;

	Assert(table != NULL);

	// Get key hash value
	hashvalue = table->HashFunction(key);
	slot 	  = hashvalue % table->SlotVolume;

	if ( list_length(table->Slots[slot]) == 0 ) {
		return UTIL_HASHTABLE_NOKEY; // No such key.
	}

	foreach(paircell, table->Slots[slot])
	{
		PAIR curnode = (PAIR)(lfirst(paircell));
		if ( table->CompFunction(curnode->Key, key) == 0 )
		{

			MEMORY_CONTEXT_SWITCH_TO(table->Context)
			table->Slots[slot] = list_delete_cell(table->Slots[slot],
												  paircell,
												  prevcell);
			MEMORY_CONTEXT_SWITCH_BACK

			if ( list_length(table->Slots[slot]) == 0 )
			{
				table->SlotCount--;
			}

			table->NodeCount--;

			/* Free key object */
			if ( curnode->Key != NULL )
			{
				table->KeyFreeFunction(table->Context, curnode->Key);
			}

			/* Free value object */
			if ( table->ValFreeFunction != NULL )
			{
				table->ValFreeFunction(curnode->Value);
			}

			/* Free pair object */
			freePAIR(curnode);

			return FUNC_RETURN_OK;
		}

		prevcell = paircell;
	}

	return UTIL_HASHTABLE_NOKEY;
}

void clearHASHTABLE( HASHTABLE table ) {

	ListCell *paircell = NULL;

	for ( int i = 0 ; i < table->SlotVolume ; ++i ) {

		foreach(paircell, table->Slots[i])
		{
			PAIR pair = (PAIR)lfirst(paircell);
			if ( pair->Key != NULL )
			{
				table->KeyFreeFunction(table->Context, pair->Key);
			}
			if ( table->ValFreeFunction != NULL )
			{
				table->ValFreeFunction(pair->Value);
			}
			freePAIR(pair);
		}

		MEMORY_CONTEXT_SWITCH_TO(table->Context)
		list_free(table->Slots[i]);
		MEMORY_CONTEXT_SWITCH_BACK
		table->Slots[i] = NULL;
	}

    table->NodeCount = 0;
    table->SlotCount = 0;
}

void cleanHASHTABLE( HASHTABLE table )
{
	Assert( table != NULL );
	clearHASHTABLE( table );				/* Clear all hash table nodes. */
	rm_pfree(table->Context, table->Slots);
	table->Slots = NULL;
}

/*
void freeHASHTABLE( HASHTABLE table )
{
	Assert( table != NULL );
	cleanHASHTABLE( table );
	rm_pfree(table->Context, table);
}
*/

uint32_t HASHTABLE_Hash_UINT32 (void *data)
{
	return *((uint32_t *)(&data));
}

uint32_t HASHTABLE_Hash_VOIDPT (void *data)
{
	return *((uint32_t *)(&data));
}

uint32_t HASHTABLE_Hash_SIMPSTR(void *data)
{
	uint32_t res = string_hash(((SimpStringPtr)data)->Str,
					   	   	   ((SimpStringPtr)data)->Len);
	return res;
}

uint32_t HASHTABLE_Hash_CHARARRAY(void *data)
{
    return DatumGetUInt32(
    		  hash_any((const unsigned char *)((SimpArrayPtr)data)->Array,
                                   	   	 (int)((SimpArrayPtr)data)->Len));
}

uint32_t HASHTABLE_Comp_UINT32 (void *val1, void *val2) {
	return (*((uint32_t *)(&val1)))-
		   (*((uint32_t *)(&val2)));
}
uint32_t HASHTABLE_Comp_VOIDPT (void *val1, void *val2) {
	return (*((uint32_t *)(&val1)))-
		   (*((uint32_t *)(&val2)));
}
uint32_t HASHTABLE_Comp_SIMPSTR(void *val1, void *val2) {
	return strcmp(((SimpString *)val1)->Str,
				  ((SimpString *)val2)->Str);
}
uint32_t HASHTABLE_Comp_CHARARRAY(void *val1, void *val2) {
	return SimpleArrayComp((SimpArrayPtr)val1,
						   (SimpArrayPtr)val2);
}

void HASHTABLE_KeyFree_UINT32 (MCTYPE context, void *key) {
	/* Do nothing. */
}

void HASHTABLE_KeyFree_VOIDPT (MCTYPE context, void *key) {
	/* Do nothing. */
}

void HASHTABLE_KeyFree_SIMPSTR(MCTYPE context, void *key) {
	freeSimpleStringContent((SimpStringPtr)key);
	rm_pfree(context, key);
}

void HASHTABLE_KeyFree_CHARARRAY(MCTYPE context, void *key) {
	freeSimpleArrayContent((SimpArrayPtr)key);
	rm_pfree(context, key);
}

void *HASHTABLE_KeyCopy_UINT32 (MCTYPE context, void *key) {
	return (void *)key;
}

void *HASHTABLE_KeyCopy_VOIDPT (MCTYPE context, void *key) {
	return (void *)key;
}

void *HASHTABLE_KeyCopy_SIMPSTR(MCTYPE context, void *key) {
	SimpString *res = (SimpString *)rm_palloc0(context, sizeof(SimpString));
	initSimpleStringWithContent( res,
								 context,
								 ((SimpString *)key)->Str,
								 ((SimpString *)key)->Len);
	return res;
}

void *HASHTABLE_KeyCopy_CHARARRAY(MCTYPE context, void *key) {
	SimpArrayPtr res = createSimpleArray(context);
	setSimpleArrayWithContent(res,
							  ((SimpArrayPtr)key)->Array,
							  ((SimpArrayPtr)key)->Len);
	return res;
}
void getAllPAIRRefIntoList(HASHTABLE table, List **list)
{
	ListCell *paircell  = NULL;

	Assert(table != NULL);
	Assert(list != NULL);

	MEMORY_CONTEXT_SWITCH_TO(table->Context)
	for ( int i = 0 ; i < table->SlotVolume ; ++i )
	{
		foreach(paircell, table->Slots[i])
		{
			*list = lappend(*list, lfirst(paircell));
		}
	}
	MEMORY_CONTEXT_SWITCH_BACK
}

void freePAIRRefList(HASHTABLE table, List **list)
{
	MEMORY_CONTEXT_SWITCH_TO(table->Context)
	list_free(*list);
	MEMORY_CONTEXT_SWITCH_BACK
	*list = NULL;
}

void extendHASHTABLESlotSize ( HASHTABLE table, int newsize )
{
	ListCell *paircell  = NULL;

	if ( newsize <= table->SlotVolume )
	{
		return;
	}

	List **newslot = (List **)rm_palloc0(table->Context,
										 sizeof(List *) * newsize);
	table->SlotCount = 0;

	for ( int i=0 ; i < table->SlotVolume ; ++i )
	{
		foreach(paircell, table->Slots[i])
		{
			PAIR pair = (PAIR)lfirst(paircell);
			uint32_t hashvalue = table->HashFunction(pair->Key);
			int newindex = hashvalue % newsize;

			if ( newslot[newindex] == NULL )
			{
				table->SlotCount++;
			}
			MEMORY_CONTEXT_SWITCH_TO(table->Context)
			newslot[newindex] = lappend(newslot[newindex], pair);
			MEMORY_CONTEXT_SWITCH_BACK
		}

		list_free(table->Slots[i]);
	}

	rm_pfree(table->Context, table->Slots);
	table->Slots = newslot;
	table->SlotVolume = newsize;
}

