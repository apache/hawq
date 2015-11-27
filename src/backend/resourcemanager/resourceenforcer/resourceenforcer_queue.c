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
 * resourceenforcer_queue.c
 *		Thread safe queue implementation for resource enforcement.
 */
#include "resourceenforcer/resourceenforcer_queue.h"
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include "postgres.h"
#include "cdb/cdbvars.h"

queue *queue_create(void)
{
	queue * q = (queue *)malloc(sizeof(queue));

	if (q == NULL)
	{
		write_log("Function queue_create out of memory");
		return NULL;
	}

    q->head = NULL;
    q->tail = NULL;
    q->size = 0;

    pthread_mutexattr_t m_atts;

    if (pthread_mutexattr_init(&m_atts) != 0)
    {
    	free(q);
    	write_log("Function pthread_mutexattr_init failed with error %d",
    			  errno);
    	return NULL;
    }

    if (pthread_mutexattr_settype(&m_atts, PTHREAD_MUTEX_ERRORCHECK) != 0)
    {
    	free(q);
    	write_log("Function pthread_mutexattr_settype failed with error %d",
    			  errno);
    	return NULL;
    }

    if (pthread_mutex_init(&(q->queue_mutex), &m_atts) != 0)
    {
    	free(q);
    	write_log("Function pthread_mutex_init failed with error %d",
    			  errno);
    	return NULL;
    }

    if (pthread_cond_init(&(q->queue_cond), NULL) != 0)
    {
    	free(q);
    	write_log("Function pthread_cond_init failed with error %d",
    			  errno);
    	return NULL;
    }

    return q;
}

void queue_destroy(queue *q)
{
	Assert(q != NULL);

	pthread_mutex_lock(&(q->queue_mutex));

	qnode *q_cur = q->head;
	qnode *q_nxt = NULL;
	while (q_cur)
	{
		q_nxt = q_cur->next;
		free(q_cur->data);
		free(q_cur);
		q_cur = q_nxt;
	}

	pthread_mutex_unlock(&(q->queue_mutex));

	free(q);
}

int enqueue(queue *q, void *data)
{
	Assert(q);
	Assert(data);

    pthread_mutex_lock(&(q->queue_mutex));
    qnode *node = (qnode *)malloc(sizeof(qnode));
    if (node == NULL)
    {
    	write_log("Function enqueue out of memory");
    	pthread_mutex_unlock(&(q->queue_mutex));
    	return -1;
    }

    node->prev = NULL;
    node->next = NULL;
    node->data = data;
    if (q->size == 0)
    {
        q->head = node;
    }
    else
    {
        q->tail->next = node;
        node->prev = q->tail;
    }
    q->tail = node;
    q->size++;

    if (q->size == 1)
    {
    	pthread_cond_signal(&(q->queue_cond));
    }

    pthread_mutex_unlock(&(q->queue_mutex));

    return 0;
}


/* timeout_ms: 10^-3 second */
void *dequeue(queue *q, int timeout_ms)
{
	Assert(q);

	struct timeval tv;
	struct timespec ts;
	int res = 0;
	
	gettimeofday(&tv, NULL);
	int64 timeout_ns = (int64)timeout_ms * 1000000;
	int64 tv_nsec = tv.tv_usec * 1000 + timeout_ns;
	int nsec = tv_nsec / 1000000000;
	tv_nsec -= nsec * 1000000000;

	ts.tv_sec = tv.tv_sec + nsec;
	/* leave in ms for this */
	ts.tv_nsec = tv_nsec;

    pthread_mutex_lock(&(q->queue_mutex));
    while (q->size == 0)
    {
    	res = pthread_cond_timedwait(&(q->queue_cond), &(q->queue_mutex), &ts);
        if (res == 0)
        {
        	continue;
        }
        else if (res == ETIMEDOUT)
        {
        	pthread_mutex_unlock(&(q->queue_mutex));
        	return NULL;
        }
        else
        {
        	write_log("Function pthread_cond_timedwait with error %d",
        			  res);
        	pthread_mutex_unlock(&(q->queue_mutex));
        	return NULL;
        }
    }

    qnode *node = q->head;
    if (NULL == node->next)
    {
        q->head = NULL;
        q->tail = NULL;
    }
    else
    {
        q->head = node->next;
        node->next->prev = NULL;
    }
    q->size--;

    pthread_mutex_unlock(&(q->queue_mutex));

    void *data = node->data;
    free(node);

    return data;
}

int queue_size(queue *q)
{
	Assert(q);

    int size = 0;
    pthread_mutex_lock(&(q->queue_mutex));
    size = q->size;
    pthread_mutex_unlock(&(q->queue_mutex));
    return size;
}
