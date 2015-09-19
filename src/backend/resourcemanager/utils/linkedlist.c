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

bool isDQueueInitialized(DQueue list)
{
	return list->Context != NULL;
}
/*
 * Free DQueue instance which must has 0 nodes in it.
 */
void freeDQueue(DQueue list) {
    
    DQueueNode tofree = NULL;
    
    Assert( list != NULL );
    /* The caller must free the data one by one, to ensure the list is empty. */
    Assert( list->NodeCount == 0 );
    
    /* Free all nodes in the free list. */
    while ( list->Free != NULL ) {
        tofree = list->Free;
        list->Free = list->Free->Next;
        rm_pfree(list->Context, tofree);
    }

    rm_pfree(list->Context, list);
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

DQueueNode getDQueueNodeByIndex(DQueue list, int index)
{
	Assert( list != NULL );
	Assert( index >= 0 );
	DQueueNode p = list->Head;
	for ( int i = 0 ; i < index ; ++i )
		p = p->Next;
	return p;
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

/*
 * Generate string information of the whole list status.
 */
void buildDQueueReport(DQueue list, char *buffer, int bufferlen)
{
    /* TODO: Check if the buffer length is enough for the report. */
    if ( list == NULL )
        sprintf( buffer, "DQueue[NULL]");
    else {
        sprintf( buffer,
                 "DQueue[Len=%d,Head=%lu,Tail=%lu,Free=%lu]",
                 list->NodeCount,
                 (unsigned long)list->Head,
                 (unsigned long)list->Tail,
                 (unsigned long)list->Free );
    }
}

/******************************************************************************
 * Implementation of DSafeQueue
 ******************************************************************************/
DSafeQueue  createDSafeQueue(MCTYPE context)
{
    DSafeQueue res = NULL;
    res = (DSafeQueue)
    	  rm_palloc0(context, sizeof(DSafeQueueData));
    initializeDSafeQueue(res, context);
    return res;
}

void initializeDSafeQueue(DSafeQueue list, MCTYPE context)
{
	initializeDQueue(&(list->Queue), context);
	pthread_mutex_init(&(list->Lock), NULL);
	/* TODO: Check mutex initialization result. */
}

/* Free one DQueue instance */
void  freeDSafeQueue(DSafeQueue list)
{
	pthread_mutex_lock(&(list->Lock));
	freeDQueue(&(list->Queue));
	pthread_mutex_unlock(&(list->Lock));
}

//void  freeDSafeQueueWithFree(DSafeQueue list, FreeObjectFuncType func)
//{
//	pthread_mutex_lock(&(list->Lock));
//	freeDQueueWithFree(&(list->Queue), func);
//	pthread_mutex_unlock(&(list->Lock));
//}

void  cleanDSafeQueue(DSafeQueue list)
{
	pthread_mutex_lock(&(list->Lock));
	cleanDQueue(&(list->Queue));
	pthread_mutex_unlock(&(list->Lock));
}

void  moveDQueueNodeOut(DSafeQueue list, DQueue target, int max, bool fromhead)
{
	DQueueNode node = NULL;
	void *data		= NULL;

	pthread_mutex_lock(&(list->Lock));
	int count = 0;
	while( count < max && list->Queue.NodeCount > 0 ) {
		if ( fromhead ) {
			/* Move node out from list head. */
			data = removeDQueueNode(&(list->Queue), list->Queue.Head);

			/* Move one free node from list to the target to reuse it. */
			node = list->Queue.Free;
			list->Queue.Free = node->Next;
			node->Prev = NULL;
			node->Data = NULL;
			node->Next = target->Free;
			target->Free = node;

			/* Linked node in target as tail. */
			insertDQueueTailNode(target, data);
		}
		else {
			/* Move node out from list tail. */
			data = removeDQueueNode(&(list->Queue), list->Queue.Tail);

			/* Move one free node from list to the target to reuse it. */
			node = list->Queue.Free;
			list->Queue.Free = node->Next;
			node->Prev = NULL;
			node->Data = NULL;
			node->Next = target->Free;
			target->Free = node;

			/* Linked node in target as head. */
			insertDQueueHeadNode(target, data);
		}
		count++;
	}

	list->QueueLength = list->Queue.NodeCount;

	pthread_mutex_unlock(&(list->Lock));
}


void  moveDQueueNodeIn(DSafeQueue list, DQueue source, bool tohead)
{
	if ( source->NodeCount == 0 )
		return;

	pthread_mutex_lock(&(list->Lock));

	if ( tohead ) {
		source->Tail->Next = list->Queue.Head;
		list->Queue.Head = source->Head;
	}
	else if ( list->Queue.NodeCount == 0 ){
		list->Queue.Head = source->Head;
		list->Queue.Tail = source->Tail;
	}
	else {
		list->Queue.Tail->Next = source->Head;
		list->Queue.Tail = source->Tail;
	}

	list->Queue.NodeCount += source->NodeCount;

	list->QueueLength = list->Queue.NodeCount;

	pthread_mutex_unlock(&(list->Lock));
}

/* Get DQueue length ( node count ) */
#define getDSafeQueueLength(list) ((list)->QueueLength)

/* Insert / Remove one node at the head of DQueue. */
void  insertDSafeQueueHeadNode(DSafeQueue list, void *data)
{
	pthread_mutex_lock(&(list->Lock));
	insertDQueueHeadNode(&(list->Queue), data);
	list->QueueLength = list->Queue.NodeCount;
	pthread_mutex_unlock(&(list->Lock));
}

void  insertDSafeQueueTailNode(DSafeQueue list, void *data)
{
	pthread_mutex_lock(&(list->Lock));
	insertDQueueTailNode(&(list->Queue), data);
	list->QueueLength = list->Queue.NodeCount;
	pthread_mutex_unlock(&(list->Lock));
}

void *removeDSafeQueueHeadNode(DSafeQueue list)
{
	pthread_mutex_lock(&(list->Lock));
	void * res = removeDQueueHeadNode(&(list->Queue));
	list->QueueLength = list->Queue.NodeCount;
	pthread_mutex_unlock(&(list->Lock));
	return res;
}

//void removeAllDSafeQueueNodes(DSafeQueue list)
//{
//	pthread_mutex_lock(&(list->Lock));
//	removeAllDQueueNodes(&(list->Queue));
//	list->QueueLength = list->Queue.NodeCount;
//	pthread_mutex_unlock(&(list->Lock));
//}

/* Generate string info of the whole list status. */
void  buildDSafeQueueReport	  (DSafeQueue list, char *buffer, int bufferlen)
{
	buildDQueueReport( &(list->Queue), buffer, bufferlen);
}

void lockDSafeQueue(DSafeQueue list)
{
	pthread_mutex_lock(&(list->Lock));
}

void unlockDSafeQueue(DSafeQueue list)
{
	pthread_mutex_unlock(&(list->Lock));
}

DQueue getDSafeQueueQueue(DSafeQueue list)
{
	return &(list->Queue);
}

void swapDQueues(DQueue list1, DQueue list2)
{
	DQueueData tmp;
	tmp.Context = list1->Context; list1->Context = list2->Context ; list2->Context = tmp.Context;
	tmp.Free = list1->Free; list1->Free = list2->Free; list2->Free = tmp.Free;
	tmp.Head = list1->Head; list1->Head = list2->Head; list2->Head = tmp.Head;
	tmp.Tail = list1->Tail; list1->Tail = list2->Tail; list2->Tail = tmp.Tail;
	tmp.NodeCount = list1->NodeCount; list1->NodeCount = list2->NodeCount ; list2->NodeCount = tmp.NodeCount;

}












