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

#include "resourceenforcer/resourceenforcer_list.h"
#include "postgres.h"
#include "cdb/cdbvars.h"

/* #define VALIDATE_LIST 1 */

#ifdef VALIDATE_LIST
static void llist_validate(llist *l) {
	Assert(l);

	lnode *cur_node = l->head;
	int size = 0;
	while (cur_node)
	{
		size++;
		Assert(cur_node->data != NULL);
		cur_node = cur_node->next;
	}
	Assert(l->size == size);
}
#endif

llist *llist_create(void)
{
	llist * l = (llist *)malloc(sizeof(llist));

	if (l == NULL)
	{
		write_log("Function llist_create out of memory");
		return NULL;
	}

	l->head = NULL;
	l->size = 0;

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

    return l;
}

lnode *llist_search(llist *l, void *p, llist_cmp cmp)
{
	Assert(l);
	Assert(p);
	Assert(cmp);

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

	lnode *cur_node = l->head;
	while (cur_node)
	{
		if (cmp(cur_node->data, p) == 0)
		{
			return cur_node;
		}
		else
		{
			cur_node = cur_node->next;
		}
	}

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

	return NULL;
}

int llist_insert(llist *l, void *d)
{
	Assert(l);
	Assert(d);

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

    lnode *node = (lnode *)malloc(sizeof(lnode));

    if (node == NULL)
    {
    	write_log("Function llist_insert out of memory");
        return -1;
    }
	
    node->prev = NULL;
    node->next = NULL;

    node->data = d;

    if (l->size == 0)
    {
        l->head = node;
    }
    else
    {
        node->next = l->head;
        l->head->prev = node;
        l->head = node;
    }

    l->size++;

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

    return 0;
}

lnode* llist_delete(llist *l, void *d, llist_cmp cmp)
{
	Assert(l);
	Assert(d);
	Assert(cmp);

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

	lnode *q = llist_search(l, d, cmp);
	if (q)
	{
		if (q->prev)
		{
			q->prev->next = q->next;

			if (q->next)
			{
				q->next->prev = q->prev;
			}
		}
		else
		{
			l->head = q->next;
			if (q->next)
			{
				q->next->prev = NULL;
			}
		}

		l->size--;

		return q;
	}

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

	return NULL;
}

void llist_destroy(llist *l)
{
	Assert(l);

#ifdef VALIDATE_LIST
	llist_validate(l);
#endif

	lnode *l_cur = l->head;
	lnode *l_nxt = NULL;
	while (l_cur)
	{
		l_nxt = l_cur->next;
		free(l_cur->data);
		free(l_cur);
		l_cur = l_nxt;
	}

	free(l);
}

int llist_size(llist *l)
{
	Assert(l);

	return l->size;
}
