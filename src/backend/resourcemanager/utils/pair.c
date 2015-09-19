#include "utils/pair.h"

void freePAIR(PAIR pair)
{
	if ( pair != NULL )
		rm_pfree(pair->Context, pair);
}

PAIR createPAIR(MCTYPE context, void *key, void *value)
{
	PAIR res = NULL;
	res = (PAIR)rm_palloc0(context, sizeof(PAIRData));
	res->Key 	= key;
	res->Value 	= value;
	res->Context = context;
	return res;
}

