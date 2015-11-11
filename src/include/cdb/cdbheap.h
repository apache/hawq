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

/*-------------------------------------------------------------------------
 *
 * cdbheap.h

 * Generic heap functions, that can be used with any
 * data structure and comparator function
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBHEAP_H
#define CDBHEAP_H

/*
 * CdbHeap element comparator function type
 *
 * Returns:
 *  -1 if aComparand < bComparand
 *   0 if aComparand == bComparand
 *   1 if aComparand > bComparand
 */
typedef int (*CdbHeapCmpFn)(void *aComparand, void *bComparand, void *context);

/*
 * CdbHeap:
 * A data structure used for sorting, especially well suited for
 * match-merge type operations.
 * The actual data is in the elements of the array heap.
 * and compared for order based on a comparator function.
 *
 * The properties of the heap are:
 *	1) 'slotArray' is an array of 'nSlotsMax' slots, allocated by the caller.
 *      The format of a slot is known only to the caller.
 *      Slots [0..nSlotsUsed-1] contain valid element data.
 *      The contents of slots [nSlotsUsed..nSlotsMax-1] is undefined.
 *	2) 'bytesPerSlot' is the slot size.  It must be a multiple of sizeof(int).
 *	3) To compare the elements of slotArray, the function
 *          (*comparator)(&slotArray[i], &slotArray[j], comparatorContext)
 *      is called, and returns -1, 0, or 1 as usual for cmp functions.
 *	4) Once the slotArray has been heapified, the "heap property" holds:
 *		    (*comparator)(&slotArray[i], &slotArray[(i-1)/2]) >= 0
 *          for i in [1..nSlotsUsed-1]
 *	   This implies that the lowest value is at index 0 of the heap.
 *
 */
typedef struct CdbHeap
{
    void	       *slotArray;
    int			    nSlotsUsed;
    int             bytesPerSlot;
    int             wordsPerSlot;
	CdbHeapCmpFn    comparator;
	void	       *comparatorContext;
    void           *tempSlot;
    int			    nSlotsMax;
    bool            ownSlotArray;
} CdbHeap;


/* Get ptr to the first element (the min if the heap property is true). */
#define CdbHeap_Min(ElementType, hp)  ( (ElementType *)((hp)->slotArray) )

/* Get ptr to slotArray[i] */
#define CdbHeap_Slot(SlotType, hp, i) \
    ( (SlotType *)( (i) * (hp)->bytesPerSlot + (char *)((hp)->slotArray) ) )

/* Get current number of elements. */
#define CdbHeap_Count(hp)   ((hp)->nSlotsUsed)

/* true if heap is empty */
#define CdbHeap_IsEmpty(hp) (CdbHeap_Count(hp) == 0)


/* Allocate and initialize a CdbHeap structure.  Caller provides slotArray. */
CdbHeap *
CdbHeap_Create(CdbHeapCmpFn     comparator,
               void            *comparatorContext,
               int              nSlotsMax,
               int              bytesPerSlot,
               void            *slotArray);

/* Free a CdbHeap structure. */
void
CdbHeap_Destroy(CdbHeap *hp);

/* Arrange elements of slotArray such that the heap property is satisfied. */
void
CdbHeap_Heapify(CdbHeap *hp, int nSlotsUsed);

/*
 * Sort elements in descending order.
 * The heap property must be satisfied on entry, but no longer holds on return.
 * Returns ptr to the first sorted element (the max).
 */
void *
CdbHeap_SortDescending(CdbHeap *hp);

/* Insert a copy of the given element into the heap. */
void
CdbHeap_Insert(CdbHeap *hp, void *newElement);

/* Delete the smallest element. */
void
CdbHeap_DeleteMin(CdbHeap *hp);

/* Delete the smallest element and insert a copy of the given element. */
void
CdbHeap_DeleteMinAndInsert(CdbHeap *hp, void* newElement);


#endif   /* CDBHEAP_H */
