#include "envswitch.h"
#include "dynrm.h"
#include "resourcemanager/resourcemanager.h"
#include "utils/kvproperties.h"

/*
 * Load hawq-site.xml content into memory as a list.
 *
 * 1) If configure file path is specified in the argument, use it.
 * 2) If configure file path is set by HAWQSITE_CONF environment variable, use it.
 *
 */
List *getHawqSiteConfigurationList(const char *hawqsitepath, MCTYPE context)
{
	char *conffile = (char *)hawqsitepath;
	if ( hawqsitepath == NULL )
	{
		conffile = getenv("HAWQSITE_CONF");
		if ( conffile == NULL )
		{
			elog(ERROR, "The environment variable HAWQSITE_CONF is not set.");
			return NULL;
		}
	}

	elog(LOG, "HAWQ reads configuration from file %s.", conffile);
	List *result = NULL;

	/* parse XML property file. */
	int res = processXMLPropertyFile(conffile, context, &result);
	if ( res != FUNC_RETURN_OK )
	{
		elog(ERROR, "Fail to parse file %s for reading configuration.", conffile);
		return NULL;
	}

	return result;
}

void freeHawqSiteConfigurationList(MCTYPE context, List **conf)
{
	if ( *conf == NULL )
	{
		return;
	}
	cleanPropertyList(context, conf);
}
