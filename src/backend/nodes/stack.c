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
/*-------------------------------------------------------------------------
 *
 * Stack.h
 * 
 * Implements a Stack of objects using lists
 *-------------------------------------------------------------------------
 */

#include "nodes/stack.h"
#include "postgres.h"

/**
 * Initializes the stack.
 */
void Init(Stack *st)
{
	Assert(st);
	st->containerList = NULL;
}

/**
 * Is stack empty?
 */
bool IsEmpty(Stack *st)
{
	return (st->containerList == NULL);
}

/**
 * Push element onto stack.
 */
void Push(Stack *st, void *element)
{
	Assert(st);
	st->containerList = lcons(element, st->containerList);
	Assert(st->containerList);
}

/**
 * Pop an element from a non-empty stack. Should be called on empty stack.
 * Returns:
 * 	returns element
 */
void* Pop(Stack *st)
{
	Assert(st);
	Assert(!IsEmpty(st));
	
	void *v = lfirst(list_head(st->containerList));
	st->containerList = list_delete_first(st->containerList);
	return v;
}

/**
 * Peek at top element from a non-empty stack. Should be called on empty stack.
 * Returns:
 * 	returns element
 */
void* Peek(Stack *st)
{
	Assert(st);
	Assert(!IsEmpty(st));
	
	void *v = lfirst(list_head(st->containerList));
	return v;
}
