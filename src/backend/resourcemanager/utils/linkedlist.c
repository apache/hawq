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

#include "utils/linkedlist.h"

#include "envswitch.h"
/********************************************************************************
 * DQueue function implementation.
 *******************************************************************************/

/* 
 * Create one DQueue, return the created DQueue instance
 */
DQueue  createDQueue(MCTYPE context)
{
    DQueue res = NULL;
    
    res = (DQueue)
    	  rm_palloc0(context, sizeof(DQueueData));
    initializeDQueue(res, context);

    return res;
}

void initializeDQueue(DQueue list, MCTYPE context)
{
    list->Context 	= context;
    list->Head 		= NULL;
    list->Tail 		= NULL;
    list->Free 		= NULL;
    list->NodeCount = 0;
}

void cleanDQueue(DQueue list)
{
	DQueueNode tofree = NULL;
	Assert( list != NULL );
	Assert( list->NodeCount == 0 );

	/* Free all nodes in the free list. */
	while ( list->Free != NULL ) {
		tofree = list->Free;
		list->Free = list->Free->Next;
		rm_pfree(list->Context, tofree);
	}

}

/*
 * Get DQueue top node data
 */
void *getDQueueHeadNodeData(DQueue list)
{
    Assert( list != NULL );
    if( list->NodeCount > 0 )
        return list->Head->Data;
    return NULL;
}

void *getDQueueNodeDataByIndex(DQueue list, int index)
{
	Assert( list != NULL );
	Assert( index >= 0 );
	DQueueNode p = list->Head;
	for ( int i = 0 ; i < index ; ++i )
		p = p->Next;
	return p->Data;
}

/* Insert one node at the head of DQueue. */
void insertDQueueHeadNode(DQueue list, void *data)
{
    DQueueNode newnode = NULL;
    
    Assert( list != NULL );
    
    /* Try to reuse the node. */
    if ( list->Free != NULL ) {
        newnode = list->Free;
        list->Free = list->Free->Next;
        newnode->Next = NULL;
        newnode->Prev = NULL;
    }
    /* Alloc new node. */
    else {
    
        newnode = (DQueueNode)
        		  rm_palloc0( list->Context,
        				  	  	    sizeof(struct DQueueNodeData));
    }
    
    newnode->Data = data;
    
    /* Link into the list. */
    if ( list->NodeCount > 0 ) {
    	newnode->Next = list->Head;
    	list->Head->Prev = newnode;
    }
    else {
    	list->Tail = newnode;
    }

    list->Head = newnode;
    list->NodeCount++;

}

/* Insert one node at the tail of DQueue. */
void insertDQueueTailNode(DQueue list, void *data)
{
    DQueueNode newnode = NULL;

    Assert( list != NULL );

    /* Try to reuse the node. */
    if ( list->Free != NULL ) {
        newnode = list->Free;
        list->Free = list->Free->Next;
        newnode->Next = NULL;
        newnode->Prev = NULL;
    }
    /* Alloc new node. */
    else {

        newnode = (DQueueNode)
        		  rm_palloc0( list->Context,
        				   	   	    sizeof(struct DQueueNodeData));
    }

    newnode->Data = data;

    /* Link into the list. */
    if ( list->NodeCount > 0 ) {
    	list->Tail->Next = newnode;
    	newnode->Prev = list->Tail;
    }
    else {
    	list->Head = newnode;
    }

    list->Tail = newnode;
    list->NodeCount++;

}

void *removeDQueueNode(DQueue list, DQueueNode node) {
    void     *res      = NULL;
    Assert( list != NULL );

    /* Remove node in different cases. */
    res = node->Data;
    list->NodeCount--;
    if ( node->Prev != NULL && node->Next != NULL ) {
    	node->Prev->Next = node->Next;
    	node->Next->Prev = node->Prev;
    	node->Next = NULL;
    	node->Prev = NULL;
    }
    else if ( node->Prev != NULL ) {
    	list->Tail = node->Prev;
    	node->Prev->Next = NULL;
    	node->Prev = NULL;
    }
    else if ( node->Next != NULL ) {
    	list->Head = node->Next;
    	node->Next->Prev = NULL;
    	node->Next = NULL;
    }
    else {
    	list->Head = NULL;
    	list->Tail = NULL;
    }

    /* Add freed node into the free list. */
    node->Next = list->Free;
    node->Data = NULL;
    list->Free = node;

    return res;

}

/* Delete one node at the head of DQueue */
void *removeDQueueHeadNode(DQueue list) {
	if ( list->NodeCount == 0 )
		return NULL;
    return removeDQueueNode(list, list->Head);
}

/* Delete one node at the tail of DQueue */
void *removeDQueueTailNode(DQueue list) {
	if ( list->NodeCount == 0 )
		return NULL;
    return removeDQueueNode(list, list->Tail);
}

/*
 * Remove all nodes in the DQueue.
 */
void removeAllDQueueNodes(DQueue list)
{
    DQueueNode p	   = NULL;
    DQueueNode nextp = NULL;
    
    Assert( list != NULL );
    
    if ( list->NodeCount == 0 )
        return;
    
    p = list->Head;
    while( p != NULL ) {
    	nextp = p->Next;
        p->Data = NULL;
        p->Prev = NULL;
        p->Next = list->Free;
        list->Free = p;
        p = nextp;
    }
    list->NodeCount = 0;
    list->Head = NULL;
    list->Tail = NULL;
}













