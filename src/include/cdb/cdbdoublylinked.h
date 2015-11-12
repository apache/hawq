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
 * cdbdoublylinked.h
 *
 *-------------------------------------------------------------------------
 */
 
#ifndef CDBDOUBLYLINKED_H
#define CDBDOUBLYLINKED_H

// Postgres makes lists way too hard.
typedef struct DoubleLinks DoubleLinks;

typedef struct DoublyLinkedHead
{
	DoubleLinks	*first;
	DoubleLinks	*last;

	int32		count;
	
} DoublyLinkedHead;

struct DoubleLinks
{
	DoubleLinks	*next;
	DoubleLinks	*prev;
};

extern void
DoublyLinkedHead_Init(
	DoublyLinkedHead		*head);

extern int32 
DoublyLinkedHead_Count(
	DoublyLinkedHead		*head);

extern void* 
DoublyLinkedHead_First(
	int 					offsetToDoubleLinks,
	DoublyLinkedHead		*head);

extern void* 
DoublyLinkedHead_Last(
	int 					offsetToDoubleLinks,
	DoublyLinkedHead		*head);

extern void*
DoublyLinkedHead_Next(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head,
	void					*ele);

extern void
DoublyLinkedHead_AddFirst(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head,
	void					*ele);

extern void*
DoublyLinkedHead_RemoveLast(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head);

extern void
DoubleLinks_Init(
	DoubleLinks		*doubleLinks);

extern void
DoubleLinks_Remove(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head,
	void					*ele);

#endif   /* CDBDOUBLYLINKED_H */
