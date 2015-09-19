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
