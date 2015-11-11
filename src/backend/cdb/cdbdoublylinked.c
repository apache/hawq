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
 * cdbdoublylinked.c
 *
 *-------------------------------------------------------------------------
 */
 
#include "postgres.h"
#include "cdb/cdbdoublylinked.h"

void
DoublyLinkedHead_Init(
	DoublyLinkedHead		*head)
{
	head->first = NULL;
	head->last = NULL;
	head->count = 0;
}

int32
DoublyLinkedHead_Count(
	DoublyLinkedHead		*head)
{
	return head->count;
}

void*
DoublyLinkedHead_First(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head)
{
	DoubleLinks		*doubleLinks;
	
	if (head->first == NULL)
	{
		Assert(head->last == NULL);
		Assert(head->count == 0);
		return NULL;
	}

	Assert(head->count > 0);

	doubleLinks = head->first;
	Assert(doubleLinks->prev == NULL);

	return ((uint8*)doubleLinks) - offsetToDoubleLinks;
}

void*
DoublyLinkedHead_Last(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head)
{
	DoubleLinks		*doubleLinks;
	
	if (head->last == NULL)
	{
		Assert(head->first == NULL);
		Assert(head->count == 0);
		return NULL;
	}

	Assert(head->count > 0);

	doubleLinks = head->last;
	Assert(doubleLinks->next == NULL);

	return ((uint8*)doubleLinks) - offsetToDoubleLinks;
}

void*
DoublyLinkedHead_Next(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head,
	void					*ele)
{
	DoubleLinks		*doubleLinks;

	doubleLinks = (DoubleLinks*)(((uint8*)ele) + offsetToDoubleLinks);

	if (head->last == doubleLinks)
	{
		Assert(doubleLinks->next == NULL);

		if (head->first == doubleLinks)
		{
			Assert(doubleLinks->prev == NULL);
			Assert(head->count == 1);
		}
		else
		{
			Assert(doubleLinks->prev != NULL);
			Assert(head->count > 1);
		}
		return NULL;
	}

	doubleLinks = doubleLinks->next;
	Assert(doubleLinks != NULL);
		
	return ((uint8*)doubleLinks) - offsetToDoubleLinks;
}

void
DoublyLinkedHead_AddFirst(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head,
	void					*ele)
{
	DoubleLinks		*doubleLinks;

	doubleLinks = (DoubleLinks*)(((uint8*)ele) + offsetToDoubleLinks);

	Assert(doubleLinks->prev == NULL);
	Assert(doubleLinks->next == NULL);

	doubleLinks->prev = NULL;

	if (head->first == NULL)
	{
		Assert(head->last == NULL);
		head->first = doubleLinks;
		head->last = doubleLinks;
	}
	else
	{
		doubleLinks->next = head->first;

		head->first->prev = doubleLinks;
		head->first = doubleLinks;
	}

	head->count++;
		
}

void*
DoublyLinkedHead_RemoveLast(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head)
{
	DoubleLinks		*doubleLinks;
	
	if (head->last == NULL)
	{
		Assert(head->first == NULL);
		return NULL;
	}

	doubleLinks = head->last;
	if (head->first == doubleLinks)
	{
		Assert(head->count == 1);
		head->first = NULL;
		head->last = NULL;
	}
	else
	{
		head->last = doubleLinks->prev;
		head->last->next = NULL;
	}

	doubleLinks->prev = NULL;
	doubleLinks->next = NULL;

	head->count--;
	Assert(head->count >= 0);

	return ((uint8*)doubleLinks) - offsetToDoubleLinks;
}

void
DoubleLinks_Init(
	DoubleLinks		*doubleLinks)
{
	doubleLinks->next = NULL;
	doubleLinks->prev = NULL;
}

void
DoubleLinks_Remove(
	int						offsetToDoubleLinks,
	DoublyLinkedHead		*head,
	void					*ele)
{
	DoubleLinks		*removeDoubleLinks;

	removeDoubleLinks = (DoubleLinks*)(((uint8*)ele) + offsetToDoubleLinks);

	if (removeDoubleLinks->prev == NULL &&
		removeDoubleLinks->next == NULL)
	{
		/*
		 * Removing the only one.
		 */
		Assert(head->first == removeDoubleLinks);
		Assert(head->last == removeDoubleLinks);
		head->first = NULL;
		head->last = NULL;
	}
	else if (removeDoubleLinks->prev == NULL)
	{
		/*
		 * Removing the first element.
		 */
		Assert(head->first == removeDoubleLinks);
		
		Assert(removeDoubleLinks->next->prev == removeDoubleLinks);
		removeDoubleLinks->next->prev = NULL;
		
		head->first = removeDoubleLinks->next;
	}
	else if (removeDoubleLinks->next == NULL)
	{
		Assert(head->last == removeDoubleLinks);
		
		/*
		 * Removing the last element.
		 */
		Assert(removeDoubleLinks->prev->next == removeDoubleLinks);
		removeDoubleLinks->prev->next = NULL;
		
		head->last = removeDoubleLinks->prev;
	}
	else
	{
		/*
		 * Removing a middle element.
		 */
		Assert(removeDoubleLinks->next->prev == removeDoubleLinks);
		removeDoubleLinks->next->prev = removeDoubleLinks->prev;

		Assert(removeDoubleLinks->prev->next == removeDoubleLinks);
		removeDoubleLinks->prev->next = removeDoubleLinks->next;
	}

	removeDoubleLinks->next = NULL;
	removeDoubleLinks->prev = NULL;

	head->count--;
	Assert(head->count >= 0);
}
