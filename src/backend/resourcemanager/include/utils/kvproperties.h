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

#ifndef _KEY_VALUE_PROPERTIES_PROCESSOR_H
#define _KEY_VALUE_PROPERTIES_PROCESSOR_H

#include "resourcemanager/envswitch.h"
#include "resourcemanager/utils/linkedlist.h"
#include "resourcemanager/utils/simplestring.h"

struct KVPropertyData {
	SimpString Key;
	SimpString Val;
};

typedef struct KVPropertyData  KVPropertyData;
typedef struct KVPropertyData *KVProperty;

int processXMLPropertyFile( const char  *filename,
							MCTYPE 		 context,
							List 	   **properties);

int findPropertyValue(List *properties, const char *key, SimpStringPtr *value);

void cleanPropertyList(MCTYPE context, List **properties);

int  PropertyKeySubstring( SimpStringPtr 	key,
						   int 				index,
						   char 		  **start,
						   int 			   *length);

KVProperty createPropertyEmpty(MCTYPE context);

KVProperty createPropertyOID(MCTYPE  		 context,
							 const char 	*tag1,
							 const char 	*tag2,
							 int			*index,
							 Oid	 		 value);

KVProperty createPropertyName(MCTYPE  		 context,
							  const char 	*tag1,
							  const char 	*tag2,
							  int			*index,
							  Name	  		 value);

KVProperty createPropertyInt8(MCTYPE  		  context,
							  const char 	 *tag1,
							  const char 	 *tag2,
							  int	 		 *index,
							  int8_t  		  value);

KVProperty createPropertyInt32(MCTYPE  		  context,
							   const char 	 *tag1,
							   const char 	 *tag2,
							   int	 		 *index,
							   int32_t  	  value);

KVProperty createPropertyBool(MCTYPE  		context,
							  const char   *tag1,
							  const char   *tag2,
							  int	 	   *index,
							  Oid	  		value);

KVProperty createPropertyString(MCTYPE  	  context,
							    const char   *tag1,
							    const char   *tag2,
							    int	 	     *index,
							    const char   *value);

KVProperty createPropertyFloat(MCTYPE		 context,
							   const char	*tag1,
							   const char   *tag2,
							   int          *index,
							   float         value);

void buildDottedPropertyNameString(SimpStringPtr 	 string,
								   const char 		*tag1,
								   const char 		*tag2,
								   int  			*index);

#endif /* _KEY_VALUE_PROPERTIES_PROCESSOR_H */
