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
