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
/*
 * hcatalogmd.h
 *
 *  Representation of hcatalog table metadata
 *  Created on: Mar 6, 2015
 *      Author: antova
 */

#ifndef HCATALOGMD_H_
#define HCATALOGMD_H_

#include "postgres.h"
#include "nodes/parsenodes.h"

/*
 * Metadata for columns in HCatalog tables
 */
typedef struct HCatalogColumn
{
	/* column name */
	char *colName;
	
	/* type name */
	char *typeName;
	
	/* type modifiers, e.g. max length or precision */
	int typeModifiers[2];
	
	/* number of type modifiers */
	int nTypeModifiers;
} HCatalogColumn;

/*
 * Metadata for HCatalog tables
 */
typedef struct HCatalogTable
{
	/* database name */
	char *dbName;

	/* table name */
	char *tableName;
	
	/* columns */
	List *columns;
} HCatalogTable;



#endif /* HCATALOGMD_H_ */
