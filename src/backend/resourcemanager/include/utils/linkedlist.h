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
bool    isDQueueInitialized(DQueue list);
/* Free one DQueue instance */
void  freeDQueue(DQueue list);
//void  freeDQueueWithFree(DQueue list, FreeObjectFuncType func);

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

//void  removeAllDQueueNodesWithFree(DQueue list, FreeObjectFuncType func);

/* Generate string info of the whole list status. */
void  buildDQueueReport	  (DQueue list, char *buffer, int bufferlen);

/* Macro for building a loop for processing each data node */
#define DQUEUE_LOOP_BEGIN(list, iter, valuetype, value) 					  \
		for( DQueueNode (iter) = getDQueueContainerHead((list)) ; 			  \
			 (iter) != NULL ; 												  \
			 (iter) = nextDQueueContainer((iter))) {						  \
			 valuetype (value) = TYPCONVERT(valuetype,((iter)->Data));

#define DQUEUE_LOOP_END														  \
		}

void swapDQueues(DQueue list1, DQueue list2);

DQueueNode getDQueueNodeByIndex(DQueue list, int index);
/******************************************************************************
 * thread-safe linked-list.
 *****************************************************************************/
struct DSafeQueueData {
	DQueueData			Queue;
	pthread_mutex_t		Lock;				/* lock for race condition.		  */
	volatile int32_t	QueueLength;
};

typedef struct DSafeQueueData     *DSafeQueue;
typedef struct DSafeQueueData     DSafeQueueData;

/* Create one DQueue */
DSafeQueue  createDSafeQueue(MCTYPE context);
void    	initializeDSafeQueue(DSafeQueue list, MCTYPE context);

/* Free one DQueue instance */
void  freeDSafeQueue(DSafeQueue list);
//void  freeDSafeQueueWithFree(DSafeQueue list, FreeObjectFuncType func);

void  cleanDSafeQueue(DSafeQueue list);

/* Get node data */
void  moveDQueueNodeOut(DSafeQueue list, DQueue target, int max, bool fromhead);
void  moveDQueueNodeIn(DSafeQueue list, DQueue source, bool tohead);

/* Get DQueue length ( node count ) for logging etc. */
#define getDSafeQueueLength(list) ((list)->QueueLength)

/* Externally lock/unlock the list. */
void lockDSafeQueue(DSafeQueue list);
void unlockDSafeQueue(DSafeQueue list);
DQueue getDSafeQueueQueue(DSafeQueue list);


/* Insert / Remove one node at the head of DQueue. */
void  insertDSafeQueueHeadNode(DSafeQueue list, void *data);
void  insertDSafeQueueTailNode(DSafeQueue list, void *data);
void *removeDSafeQueueNode	  (DSafeQueue list, DQueueNode node);
void *removeDSafeQueueHeadNode(DSafeQueue list);
void  removeAllDSafeQueueNodes(DSafeQueue list);

/* Generate string info of the whole list status. */
void  buildDSafeQueueReport	  (DSafeQueue list, char *buffer, int bufferlen);

#endif //SINGLE_DIRECTION_LINKED_LIST_H
