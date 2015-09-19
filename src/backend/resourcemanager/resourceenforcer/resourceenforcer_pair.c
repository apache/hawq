#include <stdlib.h>
#include "postgres.h"
#include "cdb/cdbvars.h"
#include "resourceenforcer/resourceenforcer_pair.h"

Pair createPair(void *key, void *value)
{
	Pair p = (Pair)malloc(sizeof(PairData));

	if (p == NULL)
	{
		write_log("Function createPair out of memory");
		return NULL;
	}

	p->Key = key;
	p->Value = value;

	return p;
}

void freePair(Pair p)
{
	if (p != NULL)
	{
		free(p);
	}
}
