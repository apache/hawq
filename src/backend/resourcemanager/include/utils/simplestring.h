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

#ifndef _SIMPLE_STRING_INCLUDED
#define _SIMPLE_STRING_INCLUDED

#include "resourcemanager/envswitch.h"

struct SimpString
{
	MCTYPE				Context;
	int32_t				Len;
	char			   *Str;
};

typedef struct SimpString  SimpString;
typedef struct SimpString *SimpStringPtr;

#define UTIL_SIMPSTR_NO_MATCH 1

SimpStringPtr createSimpleString(MCTYPE context);

/* initialize string. */
void initSimpleString(SimpStringPtr str, MCTYPE context);

void initSimpleStringWithContent( SimpStringPtr str,
					  	  	  	  MCTYPE 	    context,
					  	  	  	  char 		   *content,
					  	  	  	  int 		    length);
/* set string value. */
void setSimpleStringWithContent( SimpStringPtr  str,
					  	  	 	 const char		   *content,
					  	  	 	 int		    length);

/* free simple string with allocated content. */
void freeSimpleStringContent(SimpStringPtr str);

/* Reference one existing buffer without memory allocation. */
void setSimpleStringRef(SimpStringPtr str, const char *content, int length);

/* string operations. */
int  SimpleStringFind(SimpStringPtr str, char *target);
int  SimpleStringComp(SimpStringPtr str, char *target);
void SimpleStringCopy(SimpStringPtr str, SimpString *source);
void SimpleStringFill(SimpStringPtr str, int start, int length, uint8_t val);
bool SimpleStringEmpty(SimpStringPtr str);
int  SimpleStringLocateChar(SimpStringPtr str, char target, int *location);
/* string to the other number values. */
int  SimpleStringToInt32(SimpStringPtr str, int32_t *value);
int  SimpleStringToInt64(SimpStringPtr str, int64 *value);
int  SimpleStringToDouble(SimpStringPtr str, double *value);
int  SimpleStringToBool(SimpStringPtr str, bool *value);

/*
int  SimpleStringToInt8(SimpStringPtr str, int8_t *value);
int  SimpleStringToUInt32(SimpStringPtr str, uint32_t *value);
*/

/* convert different types of value into string. */
int SimpleStringSetOid(SimpStringPtr str, Oid value);
int SimpleStringSetName(SimpStringPtr str, Name value);
int SimpleStringSetBool(SimpStringPtr str, bool value);
int SimpleStringSetInt8(SimpStringPtr str, int8_t value);
int SimpleStringSetInt32(SimpStringPtr str, int32_t value);
int SimpleStringSetText(MCTYPE context, SimpStringPtr str, text *value);
int SimpleStringSetFloat(SimpStringPtr str, float value);

int SimpleStringToOid(SimpStringPtr str, Oid *value);

bool SimpleStringIsPercentage(SimpStringPtr str);
int  SimpleStringToPercentage(SimpStringPtr str, int8_t *value);
/* <integer>mb, <integer>gb, <integer>tb */
int  SimpleStringToStorageSizeMB(SimpStringPtr str, uint32_t *value);

int  SimpleStringToMapIndexInt8(SimpStringPtr 	str,
								char 		   *strlist,
								int 			listsize,
								int				valuewidth,
								int8_t 		   *result);

int SimpleStringStartWith(SimpStringPtr str, char *prefix);

/* Copy substring [start,end) to target simple string and return the length. */
int SimpleStringSubstring(SimpStringPtr str,
						  int 			start,
						  int 			end,
						  SimpStringPtr target);

int SimpleStringTokens(SimpStringPtr  str,
					   char           split,
					   SimpStringPtr *tokens,
					   int           *tokensize);

void freeSimpleStringTokens(SimpStringPtr   owner,
							SimpStringPtr  *tokens,
							int 			tokensize);

#define setSimpleStringNoLen(str, content) \
		setSimpleStringWithContent((str), (content), strlen((content)))

#define setSimpleStringRefNoLen(str, content) \
		setSimpleStringRef((str),(content),strlen((content)))

struct SimpArray
{
	MCTYPE				Context;
	int32_t				Len;
	char			   *Array;
};

typedef struct SimpArray  SimpArray;
typedef struct SimpArray *SimpArrayPtr;

SimpArrayPtr createSimpleArray(MCTYPE context);

/* set array value. */
void setSimpleArrayWithContent( SimpArrayPtr  array,
					  	  	 	char		 *content,
					  	  	 	int		      length);

void setSimpleArrayRef( SimpArrayPtr array,
						char *content,
						int length);
/* free simple array with allocated content. */
void freeSimpleArrayContent(SimpArrayPtr array);

int SimpleArrayComp(SimpArrayPtr array1, SimpArrayPtr array2);

#endif //_SIMPLE_STRING_INCLUDED
