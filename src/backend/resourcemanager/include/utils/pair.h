#ifndef _PAIR_VALUES_H
#define _PAIR_VALUES_H

#include "resourcemanager/envswitch.h"

struct PAIRData {
	MCTYPE Context;
	void * Key;
	void * Value;
};

typedef struct PAIRData *PAIR;
typedef struct PAIRData  PAIRData;

//#define PAIR_KEYASUINT32(pair) 		((uint32_t)((pair)->Key))
//#define PAIR_KEYASVOIDPT(pair) 		((void *)  ((pair)->Key))
//#define PAIR_KEYASSTRING(pair) ((void *)  ((pair)->Key))

//#define PAIR_VALUEASVOIDPT(pair) 	((void *)  ((pair)->Value))

PAIR createPAIR(MCTYPE context, void *key, void *value);
void freePAIR(PAIR pair);

#endif /* _PAIR_VALUES_H */
