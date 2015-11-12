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
 * cdbshareddoublylinked.h
 *
 *
 *-------------------------------------------------------------------------
 */
 
#ifndef CDBSHAREDDOUBLYLINKED_H
#define CDBSHAREDDOUBLYLINKED_H

typedef struct SharedDoublyLinkedHead
{
	int		count;
	int		first;
	int		last;
	
} SharedDoublyLinkedHead;

typedef struct SharedDoubleLinks
{
	int		index;
	int		next;
	int		prev;
	
} SharedDoubleLinks;

typedef struct SharedListBase
{
	uint8		*data;
	int			size;
	int			offsetToDoubleLinks;
	
} SharedListBase;

extern void SharedListBase_Init(
	SharedListBase		*base,
	void				*data,
	int					size,
	int					offsetToDoubleLinks);

extern void SharedDoublyLinkedHead_Init(
	SharedDoublyLinkedHead	*head);

extern void SharedDoubleLinks_Init(
	SharedDoubleLinks	*doubleLinks,
	int					index);

extern SharedDoubleLinks* SharedDoubleLinks_FromElement(	
	SharedListBase				*base,
	void						*current);

extern void* SharedListBase_ToElement(	
	SharedListBase		*base,
	int					index);

extern void* SharedDoublyLinkedHead_First(
	SharedListBase				*base,
	SharedDoublyLinkedHead		*head);

extern SharedDoubleLinks* SharedListBase_ToDoubleLinks(	
	SharedListBase		*base,
	int					index);

extern void* SharedDoubleLinks_Next(
	SharedListBase				*base,
	SharedDoublyLinkedHead		*head,
	void						*currentEle);

extern void SharedDoubleLinks_Remove(
	SharedListBase				*base,
	SharedDoublyLinkedHead		*head,
	void						*removeEle);

extern void SharedDoublyLinkedHead_AddFirst(
	SharedListBase				*base,
	SharedDoublyLinkedHead		*head,
	void						*ele);

extern void SharedDoublyLinkedHead_AddLast(
	SharedListBase				*base,
	SharedDoublyLinkedHead		*head,
	void						*ele);

extern void* SharedDoublyLinkedHead_RemoveFirst(
	SharedListBase				*base,
	SharedDoublyLinkedHead		*head);

extern void SharedDoubleLinks_AddBefore(
	SharedListBase				*base,
	SharedDoublyLinkedHead		*head,
	void						*atEle,
	void						*newEle);


#endif   /* CDBSHAREDDOUBLYLINKED_H */

