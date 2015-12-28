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
/* pipe.c */

#include "postgres.h"
#include "c.h"
#include "storage/pipe.h"

pipe_t*
pipe_init(pipe_t*thiz)
{
    pthread_mutex_init(&thiz->mutex,NULL);
    pthread_cond_init(&thiz->cond,NULL);
    thiz->head = 0;
    thiz->tail = 0;
    thiz->eof = 0;
    return thiz;
}

void
pipe_close(pipe_t*thiz)
{
    pthread_cond_destroy(&thiz->cond);
    pthread_mutex_destroy(&thiz->mutex);
}

void
pipe_write(pipe_t*thiz,const char*buffer,int size)
{
    Assert(!thiz->eof);
    pthread_mutex_lock(&thiz->mutex);
    while (size)
    {
	int s;
	if (((PIPE_BUFFER_SIZE-1)&(thiz->head+1)) == thiz->tail)
	    pthread_cond_wait(&thiz->cond,&thiz->mutex);
	Assert(((PIPE_BUFFER_SIZE-1)&(thiz->head+1)) != thiz->tail);
	s = thiz->head<thiz->tail ? thiz->tail-thiz->head-1 : thiz->tail ? PIPE_BUFFER_SIZE-thiz->head : PIPE_BUFFER_SIZE-1-thiz->head;
	if (size < s)
	    s = size;
	memcpy(thiz->buffer+thiz->head,buffer,s);
	thiz->head = (thiz->head+s)&(PIPE_BUFFER_SIZE-1);
	pthread_cond_signal(&thiz->cond);
	buffer += s;
	size -= s;
    }
    pthread_mutex_unlock(&thiz->mutex);
}

void
pipe_done_writing(pipe_t*thiz)
{
    pthread_mutex_lock(&thiz->mutex);
    thiz->eof = 1;
    pthread_cond_signal(&thiz->cond);
    pthread_mutex_unlock(&thiz->mutex);
}

int
pipe_read(pipe_t*thiz,char*buffer,int size)
{
    int old_size=size;
    pthread_mutex_lock(&thiz->mutex);
    while (size)
    {
	int s=thiz->tail<=thiz->head?thiz->head-thiz->tail:PIPE_BUFFER_SIZE-thiz->tail;
	if (s == 0)
	{
	    if (thiz->eof)
		break;
	    pthread_cond_wait(&thiz->cond,&thiz->mutex);
	    continue;
	}
	if (size < s)
	    s = size;
	memcpy(buffer,thiz->buffer+thiz->tail,s);
	thiz->tail = (thiz->tail+s)&(PIPE_BUFFER_SIZE-1);
	pthread_cond_signal(&thiz->cond);
	buffer += s;
	size -= s;
    }
    pthread_mutex_unlock(&thiz->mutex);
    return old_size-size;
}
