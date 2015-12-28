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
#ifndef PIPE_H
#define PIPE_H

#include <pthread.h>
#include <postgres.h>

#define PIPE_BUFFER_SIZE	(1<<15)

typedef struct
{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int head;
    int tail;
    int eof;
    char buffer[PIPE_BUFFER_SIZE];
}pipe_t;

extern pipe_t*pipe_init(pipe_t*thiz);
extern void pipe_close(pipe_t*thiz);
extern void pipe_write(pipe_t*thiz,const char*buffer,int size);
extern void pipe_done_writing(pipe_t*thiz);
/* pipe_read will read size or until eof. */
extern int pipe_read(pipe_t*thiz,char*buffer,int size);

#endif
