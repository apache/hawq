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

#ifndef _HAWQ_RESOURCEENFORCER_SIMPSTRING_H
#define _HAWQ_RESOURCEENFORCER_SIMPSTRING_H

/*
 * GSimpArray (Generic SimpArray) definitions and declarations
 */
struct GSimpArray
{
	int32_t		Len;
	char		*Array;
};

typedef struct GSimpArray  GSimpArray;
typedef struct GSimpArray *GSimpArrayPtr;

/* create simple array */
GSimpArrayPtr createGSimpArray(void);

/* initialize simple array */
void initGSimpArray(GSimpArrayPtr sap);

/* set value for simple array */
int setGSimpArrayWithContent(GSimpArrayPtr sap, char *content, int length);

void setGSimpArrayWithRef(GSimpArrayPtr sap, char *content, int length);

/* free simple array with allocated content */
void freeGSimpArrayContent(GSimpArrayPtr sap);

/* simple array comparison function */
int GSimpArrayComp(GSimpArrayPtr sap1, GSimpArrayPtr sap2);



/*
 * GSimpString (Generic SimpString) definitions and declarations
 */
struct GSimpString
{
	int32_t		Len;
	char		*Str;
};

typedef struct GSimpString  GSimpString;
typedef struct GSimpString *GSimpStringPtr;

#define UTIL_SIMPSTR_NO_MATCH 1

GSimpStringPtr createGSimpString(void);

/* initialize string. */
void initGSimpString(GSimpStringPtr ssp);

void initGSimpStringWithContent(GSimpStringPtr ssp, char *content, int length);

void initGSimpStringFilled(GSimpStringPtr ssp, uint8_t val, int length);

/* set string value. */
int setGSimpStringWithContent(GSimpStringPtr ssp, char *content, int length);

int setGSimpStringFilled( GSimpStringPtr ssp, uint8_t val, int length);

/* free simple string with allocated content. */
void freeGSimpStringContent(GSimpStringPtr ssp);

/* Reference one existing buffer without memory allocation. */
void setGSimpStringWithRef(GSimpStringPtr str, char *content, int length);
void clearGSimpStringRef(GSimpStringPtr str);

/* string operations. */
int  GSimpStringFind(GSimpStringPtr ssp, char *target);
int  GSimpStringComp(GSimpStringPtr ssp, char *target);
int  GSimpStringCaseComp(GSimpStringPtr ssp, char *target);
void GSimpStringCopy(GSimpStringPtr ssp, GSimpString *source);
void GSimpStringFill(GSimpStringPtr ssp, int start, int length, uint8_t val);
bool GSimpStringEmpty(GSimpStringPtr ssp);
int  GSimpStringLocateChar(GSimpStringPtr ssp, char target, int *location);
void GSimpStringReplaceChar(GSimpStringPtr ssp, char oldchar, char newchar);
int GSimpStringReplaceFirst(GSimpStringPtr ssp, char *oldstr, char *newstr);
/* string to the other number values. */
int  GSimpStringToInt8(GSimpStringPtr ssp, int8_t *value);
int  GSimpStringToInt32(GSimpStringPtr ssp, int32_t *value);
int  GSimpStringToUInt32(GSimpStringPtr ssp, uint32_t *value);
int  GSimpStringToInt64(GSimpStringPtr ssp, int64_t *value);
int  GSimpStringToDouble(GSimpStringPtr ssp, double *value);
int  GSimpStringToBool(GSimpStringPtr ssp, bool *value);

/* convert different types of value into string. */
int GSimpStringSetOid(GSimpStringPtr ssp, Oid value);
int GSimpStringSetName(GSimpStringPtr ssp, Name value);
int GSimpStringSetBool(GSimpStringPtr ssp, bool value);
int GSimpStringSetInt8(GSimpStringPtr ssp, int8_t value);
int GSimpStringSetInt32(GSimpStringPtr ssp, int32_t value);
/* int GSimpStringSetText(GSimpStringPtr ssp, text *value); */
int GSimpStringSetFloat(GSimpStringPtr ssp, float value);

int GSimpStringToOid(GSimpStringPtr ssp, Oid *value);

bool GSimpStringIsPercentage(GSimpStringPtr ssp);
int  GSimpStringToPercentage(GSimpStringPtr ssp, int8_t *value);
/* <integer>mb, <integer>gb, <integer>tb */
int  GSimpStringToStorageSizeMB(GSimpStringPtr ssp, int32_t *value);

int  GSimpStringToMapIndexInt8(GSimpStringPtr ssp,
                               char *strlist,
                               int listsize,
                               int valuewidth,
                               int8_t *result);

int GSimpStringStartWith(GSimpStringPtr ssp, char *prefix);

/* Copy substring [start,end) to target simple string and return the length. */
int  GSimpStringSubstring(GSimpStringPtr ssp,
                          int start,
                          int end,
                          GSimpStringPtr target);

int GSimpStringTokens(GSimpStringPtr ssp,
                      char split,
                      GSimpStringPtr *tokens,
                      int *tokensize);

#define setGSimpStringNoLen(ssp, content) \
		setGSimpStringWithContent((ssp), (content), strlen((content)))

#define setGSimpStringRefNoLen(ssp, content) \
		setGSimpStringWithRef((ssp),(content),strlen((content)))

/* Serialization and de-serialization */

/*-----------------------------------------------------------------------------
 *
 *  Format: ( 8-byte aligned memory block. )
 *         ---------------------------------------------
 *         |   Length(int32)     | string              |
 *         +----------------------                     +
 * 		   |                                           |
 * 		   ---------------------------------------------
 *
 *---------------------------------------------------------------------------*/

int  deserializeToGSimpString(GSimpStringPtr ssp, char *content, int *value);
int  serializeFromGSimpString(GSimpStringPtr ssp, char *content, int *value);
int  serializeSizeGSimpString(GSimpStringPtr ssp, int *value);

#endif /* _HAWQ_RESOURCEENFORCER_SIMPSTRING_H */
