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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*-------------------------------------------------------------------------
 *
 * Stack.h
 * 
 * Implements a Stack of objects using lists
 *-------------------------------------------------------------------------
 */

#ifndef Stack_H_
#define Stack_H_

#include "c.h"
#include "postgres_ext.h"
#include "nodes/pg_list.h"

typedef struct Stack
{
	List *containerList;
} Stack;

extern void Init(Stack *st);
extern void Push(Stack *st, void *element);
extern bool IsEmpty(Stack *st);
extern void *Peek(Stack *st);
extern void *Pop(Stack *st);

#endif /* Stack_H_ */
