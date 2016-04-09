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
	}

	return result;
}

void freeHawqSiteConfigurationList(MCTYPE context, List **conf)
{
	if ( *conf != NULL )
	{
		cleanPropertyList(context, conf);
	}

}
