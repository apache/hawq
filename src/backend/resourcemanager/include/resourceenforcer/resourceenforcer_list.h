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
