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
