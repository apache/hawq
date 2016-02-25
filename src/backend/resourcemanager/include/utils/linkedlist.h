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

#ifndef SINGLE_DIRECTION_LINKED_LIST_H
#define SINGLE_DIRECTION_LINKED_LIST_H

/******************************************************************************
 * OVERVIEW of double direction linked list (DQueue).
 *
 * This header file defines necessary data structures and API for operating one
 * double direction linked list (DQueue).
 *
 *****************************************************************************/

#include "resourcemanager/envswitch.h"

struct DQueueNodeData;
struct DQueueData;

typedef struct DQueueNodeData *DQueueNode;
typedef struct DQueueNodeData  DQueueNodeData;

typedef struct DQueueData     *DQueue;
typedef struct DQueueData      DQueueData;

/* The function type for freeing the contained object. */
typedef void ( * FreeObjectFuncType)(void *);


struct DQueueNodeData {
	void 		    *Data;
	DQueueNode       Next;
	DQueueNode		 Prev;
};

struct DQueueData {
	MCTYPE 			 Context;				/* Memory context for allocation */
	DQueueNode       Head;					/* Head of the list.             */
	DQueueNode       Tail;					/* Tail of the list.			 */
	DQueueNode       Free;					/* Empty node for avoiding alloc */
	int				 NodeCount;				/* Total valid node count.	     */
};

#define getDQueueContainerHead(DQueue)		((DQueue)->Head)
#define getDQueueContainerTail(DQueue)		((DQueue)->Tail)
#define nextDQueueContainer(node)			((node)->Next)
#define getDQueueContainerData(node)		((node)->Data)


/* Create one DQueue */
DQueue  createDQueue(MCTYPE context);
void    initializeDQueue(DQueue list, MCTYPE context);

void cleanDQueue(DQueue list);

/* Get node data */
void *getDQueueHeadNodeData(DQueue list);
void *getDQueueNodeDataByIndex(DQueue list, int index);

/* Get DQueue length ( node count ) */
#define getDQueueLength(list) ((list)->NodeCount)

/* Insert / Remove one node at the head of DQueue. */
void  insertDQueueHeadNode(DQueue list, void *data);
void  insertDQueueTailNode(DQueue list, void *data);
void *removeDQueueNode	  (DQueue list, DQueueNode node);
void *removeDQueueHeadNode(DQueue list);
void *removeDQueueTailNode(DQueue list);
void  removeAllDQueueNodes(DQueue list);

/* Macro for building a loop for processing each data node */
#define DQUEUE_LOOP_BEGIN(list, iter, valuetype, value) 					  \
		for( DQueueNode (iter) = getDQueueContainerHead((list)) ; 			  \
			 (iter) != NULL ; 												  \
			 (iter) = nextDQueueContainer((iter))) {						  \
			 valuetype (value) = TYPCONVERT(valuetype,((iter)->Data));

#define DQUEUE_LOOP_END														  \
		}

#endif //SINGLE_DIRECTION_LINKED_LIST_H
