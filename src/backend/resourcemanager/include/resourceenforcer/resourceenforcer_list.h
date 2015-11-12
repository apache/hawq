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

#ifndef _HAWQ_RESOURCEENFORCER_LIST_H
#define _HAWQ_RESOURCEENFORCER_LIST_H

#include <stdlib.h>

typedef struct lnode
{
    void            *data;
    struct lnode	*prev;
    struct lnode    *next;
} lnode;

typedef struct llist
{
    lnode           *head;
    int             size;
} llist;

typedef int llist_cmp(void *d1, void *d2);

llist *llist_create(void);
void llist_destroy(llist *l);
int llist_insert(llist *l, void *d);
lnode* llist_delete(llist *l, void *d, llist_cmp cmp);
lnode *llist_search(llist *l, void *d, llist_cmp cmp);
int llist_size(llist *l);

#define llist_foreach(n, l)	\
	for ((n) = (l->head); (n) != NULL; (n) = (n->next))

#define llist_lfirst(l)		((l)->data)

#endif /* _HAWQ_RESOURCEENFORCER_LIST_H */
