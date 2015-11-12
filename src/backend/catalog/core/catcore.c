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

/*-------------------------------------------------------------------------
 *
 * catcore.c
 *	  catcore access methods
 *
 * The lookup table is generated as catcoretable.c.  This file is set of
 * function to access it.
 *
 * CatCore is for general use, but currently resides under ucs because
 * it is the only client that uses CatCore.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/catcore.h"
#include "catalog/pg_type.h"


static int
catcore_relation_cmp(const void *a, const void *b)
{
	const CatCoreRelation *rel = (const CatCoreRelation *) b;

	return strcmp(a, rel->relname);
}

/*
 * Search a catcore relation by relname.
 */
const CatCoreRelation *
catcore_lookup_rel(char *relname)
{
	return (CatCoreRelation *) bsearch(relname, CatCoreRelations,
									   CatCoreRelationSize,
									   sizeof(CatCoreRelation),
									   catcore_relation_cmp);
}

/*
 * Search a catcore attribute number by attribute name.
 */
AttrNumber
catcore_lookup_attnum(const CatCoreRelation *relation, char *attname)
{
	const CatCoreAttr *attr;

	attr = catcore_lookup_attr(relation, attname);

	if (attr != NULL)
		return attr->attnum;
	else
		return InvalidAttrNumber;
}

/*
 * Search a catcore attribute by attribute number.
 * Since the number of attributes is small, linear search is probably
 * faster than binary search.
 */
const CatCoreAttr *
catcore_lookup_attr(const CatCoreRelation *relation, char *attname)
{
	int		i;

	if (relation->hasoid && strcmp(attname, "oid") == 0)
		return &TableOidAttr;

	for (i = 0; i < relation->natts; i++)
	{
		const CatCoreAttr		   *attr;

		attr = &relation->attributes[i];
		if (strcmp(attr->attname, attname) == 0)
			return attr;
	}

	return NULL;
}
