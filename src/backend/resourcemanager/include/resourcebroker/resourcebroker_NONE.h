#ifndef HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_NONE_H
#define HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_NONE_H
#include "envswitch.h"
#include "resourcebroker_API.h"
#include "dynrm.h"

/* Acquire and return resource. */
int RB_NONE_acquireResource(uint32_t memorymb, uint32_t core, List *preferred);
int RB_NONE_returnResource(List **containers);
void RB_NONE_createEntries(RB_FunctionEntries entries);
void RB_NONE_handleError(int errorcode);
#endif /* HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_NONE_H */
