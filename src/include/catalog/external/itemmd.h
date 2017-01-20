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
 * itemmd.h
 *
 *  Representation of PXF item metadata
 *  Created on: Mar 6, 2015
 *      Author: antova
 */

#ifndef ITEMMD_H_
#define ITEMMD_H_

#include "postgres.h"
#include "nodes/parsenodes.h"

/*
 * Metadata for fields in PXF items
 */
typedef struct PxfField
{
	/* column name */
	char *name;
	
	/* type name */
	char *type;
	
	/*source type name */
	char *sourceType;

	/* type modifiers, e.g. max length or precision */
	int typeModifiers[2];
	
	/* number of type modifiers */
	int nTypeModifiers;
} PxfField;

/*
 * Metadata for PXF items
 */
typedef struct PxfItem
{
	/* profile name */
	char *profile;

	/* path */
	char *path;

	/* item name */
	char *name;
	
	/* fields */
	List *fields;

	/* output formats*/
	List *outputFormats;

	int delimiter;
} PxfItem;



#endif /* ITEMMD_H_ */
