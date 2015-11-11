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

#ifndef _HAWQ_RESOURCEENFORCER_QUEUE_H
#define _HAWQ_RESOURCEENFORCER_QUEUE_H

#include <stdlib.h>
#include <pthread.h>

typedef struct qnode
{
    void            *data;
    struct qnode    *prev;
    struct qnode    *next;
} qnode;

typedef struct queue
{
    qnode           *head;
    qnode           *tail;
    int             size;
    pthread_mutex_t queue_mutex;
    pthread_cond_t  queue_cond;
} queue;

queue *queue_create(void);
int enqueue(queue *q, void *data);
void *dequeue(queue *q, int timeout_us);
void queue_destroy(queue *q);
int queue_size(queue *q);

#endif /* _HAWQ_RESOURCEENFORCER_QUEUE_H */
