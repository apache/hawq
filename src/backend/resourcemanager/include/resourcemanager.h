#ifndef _HAWQ_RESOURCE_MANAGER_H
#define _HAWQ_RESOURCE_MANAGER_H

#include "resourcemanager/envswitch.h"
#include "utils/linkedlist.h"

#define WITHLISTSTART_TAG		"withliststart"
#define WITHOUTLISTSTART_TAG	"withoutliststart"

#define HAWQSITE_HEAD_STRING    "hawq."

int ResManagerMain(int argc, char *argv[]);

int ResManagerProcessStartup(void);

List *getHawqSiteConfigurationList(const char *hawqsitepath, MCTYPE context);
void freeHawqSiteConfigurationList(MCTYPE context, List **conf);

int  loadDynamicResourceManagerConfigure(void);
#endif /* _HAWQ_RESOURCE_MANAGER_H */
