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
#include "utils/simplestring.h"

SimpStringPtr createSimpleString(MCTYPE context)
{
	SimpStringPtr res = (SimpStringPtr)
						rm_palloc0(context,
										 sizeof(SimpString));
	res->Str = NULL;
	res->Len = -1;
	res->Context = context;
	return res;
}


void initSimpleString(SimpStringPtr str, MCTYPE context)
{
	Assert( str != NULL );
	str->Str = NULL;
	str->Len = -1;
	str->Context = context;
}

void initSimpleStringWithContent( SimpStringPtr str,
					  	  	  	  MCTYPE 	    context,
					  	  	  	  char 		   *content,
					  	  	  	  int 		    length)
{
	Assert( str != NULL );
	str->Context = context;
	setSimpleStringWithContent( str, content, length);
}

void setSimpleStringWithContent( SimpStringPtr str,
					  	  	     const char		  *content,
					  	  	     int		   length)
{
	Assert( str != NULL );

	if ( str->Str == NULL ) {
		//elog(RMLOG, "SET NEW SIMPSTRING: %d::%s", length, content);

		str->Str = (char *)rm_palloc0(str->Context, length+1);
	}
	else if ( str->Len < length ) {
		//elog(RMLOG, "SET UPD SIMPSTRING: %d::%s", length, content);

		rm_pfree(str->Context, str->Str);
		str->Str = (char *)rm_palloc0(str->Context, length+1);
	}
	memcpy(str->Str, content, length);
	str->Str[length] = '\0';
	str->Len = length;
}

void freeSimpleStringContent(SimpStringPtr  str)
{
	Assert( str != NULL );
	if ( str->Str != NULL ) {
		rm_pfree(str->Context, str->Str);
	}
	str->Str = NULL;
	str->Len = -1;
}

void setSimpleStringRef(SimpStringPtr str, const char *content, int length)
{
	Assert( str != NULL );
	str->Str = (char *)content;
	str->Len = length;
	str->Context = NULL;
}

int  SimpleStringFind(SimpStringPtr str, char *target)
{
	Assert( str != NULL );
	if ( str->Str == NULL )
		return -1;
	char *res = strstr(str->Str, target);
	if ( res == NULL )
		return -1;
	return res-str->Str;
}

int  SimpleStringComp(SimpStringPtr str, char *target)
{
	Assert( str != NULL );
	return strcmp(str->Str, target);
}

void SimpleStringCopy(SimpStringPtr str, SimpStringPtr source)
{
	Assert( source != NULL );
	Assert( str != NULL );

	setSimpleStringWithContent(str, source->Str, source->Len);
}

bool SimpleStringEmpty(SimpStringPtr str)
{
	return str->Len == 0;
}

int SimpleStringSubstring( SimpStringPtr str,
						   int start,
						   int end,
						   SimpStringPtr target)
{
	Assert( str != NULL );
	Assert( target != NULL );

	/* If end == -1, the substring ends till the end of source string. */
	Assert( start >= 0 && ((end > 0 && end > start) || (end == -1)));

	int newlen = end == -1 ? str->Len-start : end-start;

	/* Check and alloc target string space. */
	setSimpleStringWithContent( target,
								str->Str+start,
								newlen);

	return newlen;
}

bool SimpleStringIsPercentage(SimpStringPtr str)
{
	if ( str->Len <= 1 )
		return false;

	if ( str->Str[strlen(str->Str)-1] == '%' )
		return true;
	return false;
}

int SimpleStringToPercentage(SimpStringPtr str, int8_t *value)
{
	int val;
	if (sscanf(str->Str,"%d%%", &val) == 1 &&
		val >= 0 &&
		val <= 100 ) {
		*value = val;
		return FUNC_RETURN_OK;
	}

	return UTIL_SIMPSTRING_WRONG_FORMAT;

}

int  SimpleStringToStorageSizeMB(SimpStringPtr str, uint32_t *value)
{
	int 	tail 	= strlen(str->Str) - 1;
	int 	scanres = -1;
	int32_t val;
	char	buff[256];

	if ( tail < 2 || tail > sizeof(buff)-1 )
		return UTIL_SIMPSTRING_WRONG_FORMAT;

	strncpy(buff, str->Str, tail-1);
	buff[tail-1] = '\0';

	scanres = sscanf(buff, "%d", &val);
	if ( scanres != 1 )
		return UTIL_SIMPSTRING_WRONG_FORMAT;

	if ( (str->Str[tail]   == 'b' || str->Str[tail] == 'B' ) &&
		 (str->Str[tail-1] == 'm' || str->Str[tail-1] == 'M') ) {
		*value = val;
	}
	else if ( (str->Str[tail]   == 'b' || str->Str[tail] == 'B' ) &&
			  (str->Str[tail-1] == 'g' || str->Str[tail-1] == 'G') ) {
		*value = val * 1024; /* GB to MB */
	}
	else if ( (str->Str[tail]   == 'b' || str->Str[tail] == 'B' ) &&
			  (str->Str[tail-1] == 't' || str->Str[tail-1] == 'T') ) {
		*value = val * 1024 * 1024; /* TB to MB */
	}
	else {
		return UTIL_SIMPSTRING_WRONG_FORMAT;
	}
	return FUNC_RETURN_OK;
}

int SimpleStringToMapIndexInt8(SimpStringPtr 	str,
							   char 		   *strlist,
							   int 				listsize,
							   int				valuewidth,
							   int8_t 		   *result)
{
	for ( int i = 0 ; i < listsize && i < 127 ; ++i ) {
		if ( SimpleStringComp(str, strlist + i * valuewidth) == 0 ) {
			*result = i & 0xFF;
			return FUNC_RETURN_OK;
		}
	}

	*result = -1;
	return UTIL_SIMPSTR_NO_MATCH;
}

int  SimpleStringToBool(SimpStringPtr str, bool *value)
{
	if ( strcmp(str->Str, "false") == 0 ||
		 strcmp(str->Str, "FALSE") == 0)
		*value = false;
	else if ( strcmp(str->Str, "true") == 0 ||
			  strcmp(str->Str, "TRUE") == 0)
		*value = true;
	else {
		return UTIL_SIMPSTRING_WRONG_FORMAT;
	}
	return FUNC_RETURN_OK;
}

/*
int  SimpleStringToInt8(SimpStringPtr str, int8_t *value)
{
	int val;
	int scanres = sscanf(str->Str, "%d", &val);
	if ( scanres == 1 && val >= -128 && val <= 127 ) {
		*value = val;
		return FUNC_RETURN_OK;
	}
	return UTIL_SIMPSTRING_WRONG_FORMAT;

}
*/

int  SimpleStringToInt32(SimpStringPtr str, int32_t *value)
{
	int32_t val;
	int scanres = sscanf(str->Str, "%d", &val);
	if ( scanres == 1 ) {
		*value = val;
		return FUNC_RETURN_OK;
	}
	return UTIL_SIMPSTRING_WRONG_FORMAT;
}

/*
int  SimpleStringToUInt32(SimpStringPtr str, uint32_t *value)
{
	uint32_t val;
	int scanres = sscanf(str->Str, "%u", &val);
	if ( scanres == 1 ) {
		*value = val;
		return FUNC_RETURN_OK;
	}
	return UTIL_SIMPSTRING_WRONG_FORMAT;
}
*/

int  SimpleStringToInt64(SimpStringPtr str, int64 *value)
{
	int64 val;
	int scanres = sscanf(str->Str, INT64_FORMAT, &val);
	if ( scanres == 1 ) {
		*value = val;
		return FUNC_RETURN_OK;
	}
	return UTIL_SIMPSTRING_WRONG_FORMAT;
}
int  SimpleStringToDouble(SimpStringPtr str, double *value)
{
	double val;
	int scanres = sscanf(str->Str, "%lf", &val);
	if ( scanres == 1 ) {
		*value = val;
		return FUNC_RETURN_OK;
	}
	return UTIL_SIMPSTRING_WRONG_FORMAT;
}

int SimpleStringStartWith(SimpStringPtr str, char *prefix)
{
	int prefixlen = strlen(prefix);
	if ( strncmp(str->Str, prefix, prefixlen) == 0 )
		return FUNC_RETURN_OK;
	return FUNC_RETURN_FAIL;
}

int  SimpleStringLocateChar(SimpStringPtr str, char target, int *location)
{
	for ( int i = 0 ; i < str->Len ; ++i ) {
		if ( str->Str[i] == target ) {
			*location = i;
			return FUNC_RETURN_OK;
		}
	}
	return FUNC_RETURN_FAIL;
}

int SimpleArrayComp(SimpArrayPtr array1, SimpArrayPtr array2)
{
	int val = 0;
	for ( int i = 0 ; i < array1->Len && i < array2->Len ; ++i ) {
		val = (uint8_t)array1->Array[i] - (uint8_t)array2->Array[i];
		if ( val != 0 )
			return val;
	}

	return (array1->Len == array2->Len) ? 0 : array1->Len - array2->Len;
}

SimpArrayPtr createSimpleArray(MCTYPE context)
{
	SimpArrayPtr res = (SimpArrayPtr) rm_palloc0(context, sizeof(SimpArray));
	res->Array   = NULL;
	res->Len 	 = -1;
	res->Context = context;
	return res;
}

/* set array value. */
void setSimpleArrayWithContent( SimpArrayPtr  array,
					  	  	 	char		 *content,
					  	  	 	int		      length)
{
	Assert( array != NULL );

	if ( array->Array == NULL ) {
		array->Array = (char *)rm_palloc0(array->Context, length+1);
	}
	else if ( array->Len < length ) {
		rm_pfree(array->Context, array->Array);
		array->Array = (char *)rm_palloc0(array->Context, length+1);
	}
	memcpy(array->Array, content, length);
	array->Len = length;
}

void setSimpleArrayRef( SimpArrayPtr  array,
						char 		 *content,
						int 		  length)
{
	Assert( array != NULL );
	array->Array = content;
	array->Len   = length;
}

/* free simple array with allocated content. */
void freeSimpleArrayContent(SimpArrayPtr array)
{
	Assert( array != NULL );
	if ( array->Array != NULL ) {
		rm_pfree(array->Context, array->Array);
	}
	array->Array 	= NULL;
	array->Len 		= -1;
}

int SimpleStringSetOid(SimpStringPtr str, Oid value)
{
	static char valuestr[64];
	Assert(str != NULL);
	int64 val = value;
	sprintf(valuestr, INT64_FORMAT, val);
	setSimpleStringNoLen(str, valuestr);
	return FUNC_RETURN_OK;
}

int SimpleStringSetName(SimpStringPtr str, Name value)
{
	Assert(str != NULL);
	setSimpleStringNoLen(str, value->data);
	return FUNC_RETURN_OK;
}

int SimpleStringSetBool(SimpStringPtr str, bool value)
{
	Assert(str != NULL);
	setSimpleStringNoLen(str, value?"true":"false");
	return FUNC_RETURN_OK;
}

int SimpleStringSetInt8(SimpStringPtr str, int8_t value)
{
	static char valuestr[8];
	Assert(str != NULL);
	sprintf(valuestr, "%d", value);
	setSimpleStringNoLen(str, valuestr);
	return FUNC_RETURN_OK;
}

int SimpleStringSetInt32(SimpStringPtr str, int32_t value)
{
	static char valuestr[8];
	Assert(str != NULL);
	sprintf(valuestr, "%d", value);
	setSimpleStringNoLen(str, valuestr);
	return FUNC_RETURN_OK;
}

int SimpleStringSetFloat(SimpStringPtr str, float value)
{
	static char valuestr[32];
	Assert(str != NULL);
	sprintf(valuestr, "%f", value);
	setSimpleStringNoLen(str, valuestr);
	return FUNC_RETURN_OK;
}

int SimpleStringToOid(SimpStringPtr str, Oid *value)
{
	int res = sscanf(str->Str, "%d", value);
	return res == 1 ? FUNC_RETURN_OK : UTIL_SIMPSTRING_WRONG_FORMAT;
}

int SimpleStringTokens( SimpStringPtr  str,
						char           split,
						SimpStringPtr *tokens,
						int           *tokensize)
{
	if ( str->Str == NULL || str->Len == 0 ) {
		*tokens = NULL;
		*tokensize = 0;
		return FUNC_RETURN_OK;
	}

	*tokensize = 1;
	for( int i = 0 ; i < str->Len ; ++i ) {
		if ( str->Str[i] == split ) {
			(*tokensize)++;
		}
	}

	*tokens = rm_palloc0(str->Context, sizeof(SimpString) * (*tokensize));
	char *prevp = str->Str;
	char *p     = str->Str;
	for ( int i = 0 ; i < *tokensize ; ++i ) {
		while( *p != split && *p != '\0' )
			p++;
		initSimpleString(&((*tokens)[i]), str->Context);
		setSimpleStringWithContent(&((*tokens)[i]), prevp, p-prevp);
		p++;
		prevp = p;
	}

	return FUNC_RETURN_OK;
}

void freeSimpleStringTokens(SimpStringPtr   owner,
							SimpStringPtr  *tokens,
							int 			tokensize)
{
	for ( int i = 0 ; i < tokensize ; ++i )
	{
		freeSimpleStringContent(&((*tokens)[i]));
	}
	rm_pfree(owner->Context, *tokens);
	*tokens = NULL;
}
