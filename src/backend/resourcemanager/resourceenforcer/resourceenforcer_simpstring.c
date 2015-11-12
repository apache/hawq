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

#include <stdlib.h>
#include "postgres.h"
#include "fmgr.h"
#include "cdb/cdbvars.h"
#include "resourcemanager/errorcode.h"
#include "resourceenforcer/resourceenforcer_simpstring.h"

/*
 * GSimpArray implementation
 */

/* create simple array */
GSimpArrayPtr createGSimpArray(void)
{
	GSimpArrayPtr sap = (GSimpArrayPtr)malloc(sizeof(GSimpArray));

	if ( sap == NULL )
	{
		write_log("Function createGSimpArray out of memory");
		return NULL;
	}

	sap->Array	= NULL;
	sap->Len	= -1;

	return sap;
}

/* initialize simple array */
void initGSimpArray(GSimpArrayPtr sap)
{
	Assert( sap != NULL );

	sap->Array	= NULL;
	sap->Len	= -1;
}

/* set value for simple array */
int setGSimpArrayWithContent(GSimpArrayPtr sap, char *content, int length)
{
	Assert( sap != NULL );

	if ( sap->Array == NULL )
	{
		sap->Array = (char *)malloc(length+1);
	}
	else if ( sap->Len < length )
	{
		free(sap->Array);
		sap->Array = (char *)malloc(length+1);
	}

	if ( sap->Array == NULL )
	{
		write_log("Function setGSimpArrayWithContent out of memory");
		return UTIL_SIMPSTRING_OUT_OF_MEMORY;
	}

	memcpy(sap->Array, content, length);
	sap->Len = length;

	return FUNC_RETURN_OK;
}

void setGSimpArrayWithRef(GSimpArrayPtr sap, char *content, int length)
{
	Assert( sap != NULL );

	sap->Array	= content;
	sap->Len	= length;
}

/* free simple array with allocated content. */
void freeGSimpArrayContent(GSimpArrayPtr sap)
{
	Assert( sap != NULL );

	if ( sap->Array != NULL )
	{
		free(sap->Array);
	}

	sap->Array	= NULL;
	sap->Len	= -1;
}

/* simple array comparison function */
int GSimpArrayComp(GSimpArrayPtr sap1, GSimpArrayPtr sap2)
{
	int val = 0;

	for ( int i = 0 ; i < sap1->Len && i < sap2->Len ; ++i )
	{
		val = (uint8_t)sap1->Array[i] - (uint8_t)sap2->Array[i];

		if ( val != 0 )
		{
			return val;
		}
	}

	return (sap1->Len - sap2->Len);
}



/*
 * GSimpString implementation
 */

GSimpStringPtr createGSimpString(void)
{
	GSimpStringPtr ssp = (GSimpStringPtr)malloc(sizeof(GSimpString));

	if ( ssp == NULL )
	{
		write_log("Function createGSimpString out of memory");
		return NULL;
	}

	ssp->Str = NULL;
	ssp->Len = -1;

	return ssp;
}

void initGSimpString(GSimpStringPtr ssp)
{
	Assert( ssp != NULL );

	ssp->Str = NULL;
	ssp->Len = -1;
}

void initGSimpStringWithContent(GSimpStringPtr ssp, char *content, int length)
{
	Assert( ssp != NULL );

	setGSimpStringWithContent( ssp, content, length);
}

void initGSimpStringFilled(GSimpStringPtr ssp, uint8_t val, int length)
{
	Assert( ssp != NULL );

	setGSimpStringFilled( ssp, val, length);
}

int setGSimpStringWithContent(GSimpStringPtr ssp, char *content, int length)
{
	Assert( ssp != NULL );

	if ( ssp->Str == NULL )
	{
		ssp->Str = (char *)malloc(length+1);
	}
	else if ( ssp->Len < length )
	{
		free(ssp->Str);

		ssp->Str = (char *)malloc(length+1);
	}

	if ( ssp->Str == NULL )
	{
		write_log("Function setGSimpStringWithContent out of memory");
		return UTIL_SIMPSTRING_OUT_OF_MEMORY;
	}

	memcpy(ssp->Str, content, length);
	ssp->Str[length] = '\0';
	ssp->Len = length;

	return FUNC_RETURN_OK;
}

int setGSimpStringFilled(GSimpStringPtr ssp, uint8_t val, int length)
{
	Assert( ssp != NULL );

	ssp->Str = (char *)malloc(length+1);

	if ( ssp->Str == NULL )
	{
		write_log("Function setGSimpStringFilled out of memory");
		return UTIL_SIMPSTRING_OUT_OF_MEMORY;
	}

	memset(ssp->Str, val, length);
	ssp->Len = length;

	return FUNC_RETURN_OK;
}

void freeGSimpStringContent(GSimpStringPtr ssp)
{
	Assert( ssp != NULL );

	if ( ssp->Str != NULL )
	{
		free(ssp->Str);
	}

	ssp->Str = NULL;
	ssp->Len = -1;
}

void setGSimpStringWithRef(GSimpStringPtr ssp, char *content, int length)
{
	Assert( ssp != NULL );

	ssp->Str = content;
	ssp->Len = length;
}

void clearGSimpStringRef(GSimpStringPtr ssp)
{
	Assert( ssp != NULL );

	ssp->Str = NULL;
	ssp->Len = -1;
}

int GSimpStringFind(GSimpStringPtr ssp, char *target)
{
	Assert( ssp != NULL );
	Assert( target );

	if ( ssp->Str == NULL )
	{
		return -1;
	}

	char *res = strstr(ssp->Str, target);

	if ( res == NULL )
	{
		return -1;
	}

	return res-(ssp->Str);
}

int GSimpStringComp(GSimpStringPtr ssp, char *target)
{
	Assert( ssp != NULL );

	return strcmp(ssp->Str, target);
}

int GSimpStringCaseComp(GSimpStringPtr ssp, char *target)
{
	Assert( ssp != NULL );

	return strcasecmp(ssp->Str, target);
}

void GSimpStringCopy(GSimpStringPtr ssp, GSimpStringPtr source)
{
	Assert( ssp != NULL );
	Assert( source != NULL );

	setGSimpStringWithContent(ssp, source->Str, source->Len);
}

bool GSimpStringEmpty(GSimpStringPtr ssp)
{
	return ssp->Len == 0;
}

int GSimpStringSubstring(GSimpStringPtr ssp,
                         int start,
                         int end,
                         GSimpStringPtr target)
{
	Assert( ssp != NULL );
	Assert( target != NULL );

	/* If end == -1, the substring ends till the end of source string. */
	Assert( start >= 0 && ((end > 0 && end > start) || (end == -1)));

	int newlen = end == -1 ? (ssp->Len)-start : end-start;

	/* Check and alloc target string space. */
	setGSimpStringWithContent( target,
                               (ssp->Str)+start,
                               newlen);

	return newlen;
}

int deserializeToGSimpString(GSimpStringPtr ssp, char *content, int *value)
{
	Assert( ssp );
	Assert( content );
	Assert( value );

	ssp->Len = *((int32_t *)content);
	ssp->Str = (char *)malloc(ssp->Len+1);

	if ( ssp->Str == NULL )
	{
		write_log("Function deserializeToGSimpString out of memory");
		return UTIL_SIMPSTRING_OUT_OF_MEMORY;
	}

	memcpy(ssp->Str, content+4, ssp->Len);

	*value =  __SIZE_ALIGN64(ssp->Len + sizeof(int32_t));

	return FUNC_RETURN_OK;
}

int serializeFromGSimpString(GSimpStringPtr ssp, char *content, int *value)
{
	Assert( ssp );
	Assert( content );
	Assert( value );

	*((int32_t *)content) = ssp->Len;
	memcpy(content+4, ssp->Str, ssp->Len);

	*value = __SIZE_ALIGN64(ssp->Len + sizeof(int32_t));

	return FUNC_RETURN_OK;
}

int serializeSizeGSimpString(GSimpStringPtr ssp, int *value)
{
	Assert( ssp );
	Assert( value );

	*value = __SIZE_ALIGN64(ssp->Len + sizeof(int32_t));

	return FUNC_RETURN_OK;
}

bool GSimpStringIsPercentage(GSimpStringPtr ssp)
{
	Assert( ssp );

	if ( ssp->Len <= 1 )
	{
		return false;
	}

	if ( ssp->Str[strlen(ssp->Str)-1] == '%' )
	{
		return true;
	}

	return false;
}

int GSimpStringToPercentage(GSimpStringPtr ssp, int8_t *value)
{
	Assert( ssp );
	Assert( value );

	int val;

	if (sscanf(ssp->Str,"%d%%", &val) == 1 &&
		val >= 0 &&
		val <= 100 )
	{
		*value = val;

		return FUNC_RETURN_OK;
	}

	return UTIL_SIMPSTRING_WRONG_FORMAT;
}

int GSimpStringToStorageSizeMB(GSimpStringPtr ssp, int32_t *value)
{
	Assert( ssp );
	Assert( value );

	int 	tail 	= strlen(ssp->Str) - 1;
	int 	scanres = -1;
	int32_t val;
	char	buff[256];

	if ( tail < 2 || tail > sizeof(buff)-1 )
	{
		return UTIL_SIMPSTRING_WRONG_FORMAT;
	}

	strncpy(buff, ssp->Str, tail-1);
	buff[tail-1] = '\0';

	scanres = sscanf(buff, "%d", &val);
	if ( scanres != 1 )
	{
		return UTIL_SIMPSTRING_WRONG_FORMAT;
	}

	if ( (ssp->Str[tail]   == 'b' || ssp->Str[tail] == 'B' ) &&
		 (ssp->Str[tail-1] == 'm' || ssp->Str[tail-1] == 'M') )
	{
		*value = val;
	}
	else if ( (ssp->Str[tail]   == 'b' || ssp->Str[tail] == 'B' ) &&
			  (ssp->Str[tail-1] == 'g' || ssp->Str[tail-1] == 'G') )
	{
		*value = val * 1024; /* GB to MB */
	}
	else if ( (ssp->Str[tail]   == 'b' || ssp->Str[tail] == 'B' ) &&
			  (ssp->Str[tail-1] == 't' || ssp->Str[tail-1] == 'T') )
	{
		*value = val * 1024 * 1024; /* TB to MB */
	}
	else
	{
		return UTIL_SIMPSTRING_WRONG_FORMAT;
	}

	return FUNC_RETURN_OK;
}

int GSimpStringToMapIndexInt8(GSimpStringPtr ssp,
                              char *strlist,
                              int listsize,
                              int valuewidth,
                              int8_t *result)
{
	Assert( ssp );
	Assert( strlist );
	Assert( result );

	for ( int i = 0 ; i < listsize && i < 127 ; ++i )
	{
		if ( GSimpStringComp(ssp, strlist + i * valuewidth) == 0 )
		{
			*result = i & 0xFF;

			return FUNC_RETURN_OK;
		}
	}

	*result = -1;

	return UTIL_SIMPSTR_NO_MATCH;
}

int GSimpStringToBool(GSimpStringPtr ssp, bool *value)
{
	Assert( ssp );
	Assert( value );

	if ( strcmp(ssp->Str, "false") == 0 ||
		 strcmp(ssp->Str, "FALSE") == 0)
	{
		*value = false;
	}
	else if ( strcmp(ssp->Str, "true") == 0 ||
			  strcmp(ssp->Str, "TRUE") == 0)
	{
		*value = true;
	}
	else
	{
		return UTIL_SIMPSTRING_WRONG_FORMAT;
	}

	return FUNC_RETURN_OK;
}

int GSimpStringToInt8(GSimpStringPtr ssp, int8_t *value)
{
	Assert( ssp );
	Assert( value );

	int val;
	int scanres = sscanf(ssp->Str, "%d", &val);

	if ( scanres == 1 && val >= -128 && val <= 127 )
	{
		*value = val;

		return FUNC_RETURN_OK;
	}

	return UTIL_SIMPSTRING_WRONG_FORMAT;

}

int GSimpStringToInt32(GSimpStringPtr ssp, int32_t *value)
{
	Assert( ssp );
	Assert( value );

	int32_t val;
	int scanres = sscanf(ssp->Str, "%d", &val);

	if ( scanres == 1 )
	{
		*value = val;
		return FUNC_RETURN_OK;
	}

	return UTIL_SIMPSTRING_WRONG_FORMAT;
}

int GSimpStringToUInt32(GSimpStringPtr ssp, uint32_t *value)
{
	Assert( ssp );
	Assert( value );

	uint32_t val;
	int scanres = sscanf(ssp->Str, "%u", &val);

	if ( scanres == 1 )
	{
		*value = val;
		return FUNC_RETURN_OK;
	}

	return UTIL_SIMPSTRING_WRONG_FORMAT;
}

int GSimpStringToInt64(GSimpStringPtr ssp, int64_t *value)
{
	Assert( ssp );
	Assert( value );

	int64_t val;
	int scanres = sscanf(ssp->Str, INT64_FORMAT, &val);

	if ( scanres == 1 )
	{
		*value = val;
		return FUNC_RETURN_OK;
	}

	return UTIL_SIMPSTRING_WRONG_FORMAT;
}

int GSimpStringToDouble(GSimpStringPtr ssp, double *value)
{
	Assert( ssp );
	Assert( value );

	double val;
	int scanres = sscanf(ssp->Str, "%lf", &val);

	if ( scanres == 1 )
	{
		*value = val;
		return FUNC_RETURN_OK;
	}

	return UTIL_SIMPSTRING_WRONG_FORMAT;
}

int GSimpStringStartWith(GSimpStringPtr ssp, char *prefix)
{
	Assert( ssp );
	Assert( prefix );

	int prefixlen = strlen(prefix);

	if ( strncmp(ssp->Str, prefix, prefixlen) == 0 )
	{
		return FUNC_RETURN_OK;
	}

	return FUNC_RETURN_FAIL;
}

int GSimpStringLocateChar(GSimpStringPtr ssp, char target, int *location)
{
	Assert( ssp );
	Assert( location );

	for ( int i = 0 ; i < ssp->Len ; ++i )
	{
		if ( ssp->Str[i] == target )
		{
			*location = i;
			return FUNC_RETURN_OK;
		}
	}

	return FUNC_RETURN_FAIL;
}

void GSimpStringReplaceChar(GSimpStringPtr ssp, char oldchar, char newchar)
{
	Assert( ssp && ssp->Str );

	for ( int i = 0 ; i < ssp->Len ; ++i )
	{
		ssp->Str[i] = ssp->Str[i] == oldchar ? newchar : ssp->Str[i];
	}
}

int GSimpStringReplaceFirst(GSimpStringPtr ssp, char *oldstr, char *newstr)
{
	Assert( ssp );
	Assert( oldstr );
	Assert( newstr );

	char *pos = strstr(ssp->Str, oldstr);

	/* If the old string does not exist, no need to do any update. */
	if ( pos == NULL )
	{
		return FUNC_RETURN_OK;
	}

	int  nlen = ssp->Len + strlen(newstr) - strlen(oldstr);
	char *buf = (char *)malloc(nlen+1);

	if ( buf == NULL )
	{
		write_log("Function GSimpStringReplaceFirst out of memory");
		return UTIL_SIMPSTRING_OUT_OF_MEMORY;
	}

	memset(buf, 0, sizeof(char)*(nlen+1));

	int  tlen1 = pos - ssp->Str;
	if ( tlen1 > 0 )
	{
		strncpy(buf, ssp->Str, tlen1);
		buf[tlen1] = '\0';
	}

	strncpy(buf + tlen1, newstr, strlen(newstr));

	int tlen2 = tlen1 + strlen(oldstr);
	int tlen3 = tlen1 + strlen(newstr);
	if ( tlen2 < ssp->Len )
	{
		strncpy(buf + tlen3,
				ssp->Str + tlen2,
				ssp->Len - tlen2);
	}
	buf[nlen] = '\0';

	setGSimpStringWithContent(ssp, buf, nlen);

	free(buf);

	return FUNC_RETURN_OK;
}

int GSimpStringSetOid(GSimpStringPtr ssp, Oid value)
{
	Assert( ssp != NULL );

	static char valuestr[64];
	int64_t val = value;
	sprintf(valuestr, INT64_FORMAT, val);
	setGSimpStringNoLen(ssp, valuestr);

	return FUNC_RETURN_OK;
}

int GSimpStringSetName(GSimpStringPtr ssp, Name value)
{
	Assert( ssp != NULL );

	setGSimpStringNoLen(ssp, value->data);

	return FUNC_RETURN_OK;
}

int GSimpStringSetBool(GSimpStringPtr ssp, bool value)
{
	Assert( ssp != NULL );

	setGSimpStringNoLen(ssp, value ? "true" : "false");

	return FUNC_RETURN_OK;
}

int GSimpStringSetInt8(GSimpStringPtr ssp, int8_t value)
{
	Assert( ssp != NULL );

	static char valuestr[8];
	sprintf(valuestr, "%d", value);
	setGSimpStringNoLen(ssp, valuestr);

	return FUNC_RETURN_OK;
}

int GSimpStringSetInt32(GSimpStringPtr ssp, int32_t value)
{
	Assert( ssp != NULL );

	static char valuestr[8];
	sprintf(valuestr, "%d", value);
	setGSimpStringNoLen(ssp, valuestr);

	return FUNC_RETURN_OK;
}

int GSimpStringSetFloat(GSimpStringPtr ssp, float value)
{
	Assert( ssp != NULL );

	static char valuestr[32];
	sprintf(valuestr, "%f", value);
	setGSimpStringNoLen(ssp, valuestr);

	return FUNC_RETURN_OK;
}

/**
 * Not supported in generic SimpString
int GSimpStringSetText(GSimpStringPtr ssp, text *value)
{
	Assert(ssp != NULL);

	if ( value != NULL )
	{
		char *tmpvalue = NULL;

		tmpvalue = DatumGetCString(DirectFunctionCall1(textout,
                                                       PointerGetDatum(value)));
		setGSimpStringNoLen(ssp, tmpvalue);
		free(tmpvalue);
	}
	else
	{
		setGSimpStringNoLen(ssp, "");
	}

	return FUNC_RETURN_OK;
}
 *
 */

int GSimpStringToOid(GSimpStringPtr ssp, Oid *value)
{
	Assert( ssp );
	Assert( value );

	int res = sscanf(ssp->Str, "%d", value);

	return res == 1 ? FUNC_RETURN_OK : UTIL_SIMPSTRING_WRONG_FORMAT;
}

int GSimpStringTokens(GSimpStringPtr ssp,
                       char split,
                       GSimpStringPtr *tokens,
                       int *tokensize)
{
	Assert( ssp );
	Assert( tokens );
	Assert( tokensize );

	if ( ssp->Str == NULL || ssp->Len == 0 )
	{
		*tokens = NULL;
		*tokensize = 0;
		return FUNC_RETURN_OK;
	}

	*tokensize = 1;
	for( int i = 0 ; i < ssp->Len ; ++i )
	{
		if ( ssp->Str[i] == split )
		{
			(*tokensize)++;
		}
	}

	*tokens = malloc(sizeof(GSimpString) * (*tokensize));
	if ( *tokens == NULL )
	{
		write_log("Function GSimpStringTokens out of memory");
		return UTIL_SIMPSTRING_OUT_OF_MEMORY;
	}

	char *prevp = ssp->Str;
	char *p     = ssp->Str;
	for ( int i = 0 ; i < *tokensize ; ++i )
	{
		while( *p != split && *p != '\0' )
		{
			p++;
		}

		initGSimpString(&((*tokens)[i]));
		setGSimpStringWithContent(&((*tokens)[i]), prevp, p-prevp);
		p++;
		prevp = p;
	}

	return FUNC_RETURN_OK;
}
