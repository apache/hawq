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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * cdbheaptest.c
 *
 */
#include "postgres.h"

#include <assert.h>
#include <ctype.h>
#include "fmgr.h"
#include "cdb/cdbheap.h"
#include "cdbheaptest.h"

typedef struct
{
	int *value;
	int source;
}HeapValue;

static int cmpIntP(void *lhs, void *rhs);
static void *createIntP(void *nothing, void *data);
static void copyIntP(void *dst, void *src);
static void switchIntP(void *left, void *right);
static int cmpint( const void *lhs, const void *rhs );
static int GetNextValueForSlot( HeapValue *values, int slot, int startIndex, int stopIndex );

int cmpIntP(void *lhs, void *rhs)
{
	HeapValue *l;
	HeapValue *r;
	int cmp;

	Assert( lhs != NULL && rhs != NULL );
	l = (HeapValue *)lhs;
	r = (HeapValue *)rhs;

	if ( l->value == NULL )
	{
		if ( r->value == NULL )
			return 0;

		return 1;
	}

	if ( r->value == NULL )
		return -1;
	

	cmp = *(l->value) - *(r->value);
	if ( cmp < 0 )
		return -1;
	else if ( cmp == 0 )
		return 0;
	else
		return 1;
}

void *createIntP(void *nothing, void *data)
{
	HeapValue	*p;
	HeapValue	*heapdata;

	Assert( data != NULL );
	heapdata = (HeapValue *)data;

	p = palloc0(sizeof(HeapValue));
	p->value = heapdata->value;
	p->source = heapdata->source;

	return (void *)p;
}

void copyIntP(void *dst, void *src)
{
	HeapValue *l;
	HeapValue *r;

	Assert( dst != NULL && src != NULL );

	l = (HeapValue *)dst;
	r = (HeapValue *)src;

	l->value = r->value;
	l->source = r->source;
}

void switchIntP(void *left, void *right)
{
	HeapValue *l;
	HeapValue *r;
	int* ptmp;
	int  tmp;

	Assert( left != NULL && right != NULL );

	l = (HeapValue *)left;
	r = (HeapValue *)right;

	ptmp = l->value;
	l->value = r->value;
	r->value = ptmp;

	tmp = l->source;
	l->source = r->source;
	r->source = tmp;
}

int cmpint( const void *lhs, const void *rhs )
{
	return ((*(int *)lhs) - (*(int *)rhs));
}

/*
 * cdb_heap_test takes 2 arguments:
 *  1) the number of elements in the heap
 *  2) the total number of integers to randomly generate
 */
PG_FUNCTION_INFO_V1(cdb_heap_test);
Datum
cdb_heap_test(PG_FUNCTION_ARGS)
{
	int		nSlots		= PG_GETARG_INT32(0);
	int		nToGenerate = PG_GETARG_INT32(1);
	int		seed		= PG_GETARG_INT32(2);
	bool	result = true;
	CdbHeap *cdbheap = NULL;
	int i;
	int *intvals;
	HeapValue* values;
	int *slotcounts;
	int *slotpositions;
	HeapValue *phv;

	Assert(nSlots >= 1 && nToGenerate >= 1 );

	cdbheap = CreateCdbHeap( nSlots, 
							cmpIntP,
							createIntP,
							copyIntP,
							switchIntP,
							NULL);
	/*
	 * Generate nToGenerate random integers
	 */
	intvals = (int *)palloc0( nToGenerate * sizeof(int) );
	srand( seed );

	for ( i=0; i<nToGenerate; i++ )
	{
		intvals[i] = rand();
	}

	// Now sort these
	qsort( intvals, nToGenerate, sizeof(int), cmpint );

	// Now allocate nSlots arrays for the sorted ints, and an array for the counts
	values = (HeapValue *)palloc0( nToGenerate * sizeof(HeapValue));
	slotcounts = (int *)palloc0( nSlots * sizeof(int) );
	slotpositions = (int *)palloc0( nSlots * sizeof(int) );

	// Now randomly assign the values to nSlots, preserving order
	for ( i=0; i<nToGenerate; i++ )
	{
		int slot = floor( ((double)rand() / ((double)RAND_MAX + 1 )) * nSlots);
		Assert( slot >= 0 && slot < nSlots);

		values[i].source = slot;
		values[i].value = intvals + i;
		slotcounts[slot]++;
	}

	elog( DEBUG4, "Slot counts follow" );
	int sum = 0;
	for ( i=0; i<nSlots; i++ )
	{
		if ( slotcounts[i] == 0 )
		{
			ereport(NOTICE,
					(errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
						errmsg("Function cdb_heap_test() cannot proceed, because not all slots got values."))
					);
			result = false;
			goto end;
		}

		sum += slotcounts[i];
		elog( DEBUG4, "slotcount[%d] = %d" , i, slotcounts[i]);
	}

	elog( DEBUG4, "slotcount total = %d" , sum);

	// Now add the first element from each slot to the heap.
	for ( i=0; i<nSlots; i++ )
	{
		int index = GetNextValueForSlot( values, i, slotpositions[i], nToGenerate );
		SetCdbHeapInitialValue( cdbheap, &values[index] );
		slotpositions[i] = index+1;
	}

	// Now grab lowest element from heap, and ask for the next element from the
	// same slot
	int lastval = INT_MIN;
	int lastslot;
	for ( i=0; i<nToGenerate; i++ )
	{
		HeapValue *phv = (HeapValue *)GetLowestValueFromCdbHeap( cdbheap );
		Assert( phv != NULL );
		if ( phv->value == NULL )
		{
			ereport(NOTICE,
					(errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
						errmsg("Function cdb_heap_test() failed. At index %d, value was NULL",
						i ))
					);
			result = false;
			goto end;
		}

		if ( lastval > *phv->value )
		{
			ereport(NOTICE,
					(errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
						errmsg("Function cdb_heap_test() failed. At index %d, value %d was smaller than previous value %d",
						i, *phv->value, lastval ))
					);
			result = false;
			goto end;
		}

		lastval = *phv->value;
		lastslot = phv->source;

		int index = GetNextValueForSlot( values, lastslot, slotpositions[lastslot], nToGenerate );

		if ( index == -1 )
			phv->value = NULL;
		else
		{
			phv = &values[index];
			slotpositions[lastslot] = index + 1;
		}

		MergeNewValueIntoCdbHeap( cdbheap, phv );
	}

	phv = GetLowestValueFromCdbHeap( cdbheap );
	Assert( phv != NULL && phv->value == NULL );

end:
	if ( cdbheap != NULL )
		DestroyCdbHeap( cdbheap );

	PG_RETURN_BOOL(result);
}

int GetNextValueForSlot( HeapValue *values, int slot, int startIndex, int stopIndex )
{
	int i;

	for ( i=startIndex; i<stopIndex; i++ )
	{
		if ( values[i].source == slot )
		{
			return i;
		}	
	}

	return -1;
}
