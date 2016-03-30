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

#include "utils/kvproperties.h"

#include <libxml/xmlreader.h>

#include "resourcemanager/resourcemanager.h"

void processXMLNode(xmlTextReaderPtr reader, MCTYPE context, List **properties);

/* Global variables for tracking parsing status */
int countConfiguration;		/* 1 means in <configuration> element.            */
int countProperty;			/* 0 means out of <property> element.			  */
							/* 1 means in <property> element. 				  */
int countName;
int countValue;
int countDesc;
int resultCode;
int posNodeLine;
int posNodeColumn;

SimpString currentNameString;
SimpString currentValueString;

void processXMLNode(xmlTextReaderPtr reader, MCTYPE context, List **properties)
{
    const xmlChar *name  	   = NULL;
    const xmlChar *value 	   = NULL;
    int	  		   xmlDepth	   = -1;
    int			   xmlType	   = -1;
    int			   xmlIsEmpty  = -1;
    int			   xmlHasValue = -1;
    KVProperty	   newproperty = NULL;

    /* Get name */
    name = xmlTextReaderConstName(reader);
    if (name == NULL)
    	name = BAD_CAST "--";

    /* Get value */
    value = xmlTextReaderConstValue(reader);

    /* Get properties of XML node */
    xmlDepth    	= xmlTextReaderDepth(reader);
    xmlType     	= xmlTextReaderNodeType(reader);
    xmlIsEmpty  	= xmlTextReaderIsEmptyElement(reader);
    xmlHasValue 	= xmlTextReaderHasValue(reader);
    posNodeLine 	= xmlTextReaderGetParserLineNumber(reader);
    posNodeColumn 	= xmlTextReaderGetParserColumnNumber(reader);

    /* Sensitive to <configuration> element. */
    if ( strcmp((const char *)name, "configuration") == 0 )
    {
    	if ( xmlDepth == 0 && xmlType  == XML_READER_TYPE_ELEMENT )
    	{
    		countConfiguration++;
    	}
    	else if ( xmlDepth == 0 && xmlType == XML_READER_TYPE_END_ELEMENT )
    	{
    		countConfiguration--;
    	}
    	else
    	{
    		/* Wrong level or wrong node type. */
    		resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_CONFIGURATION;
    		elog(WARNING, "Wrong element <configuration> is parsed.");
    		return;
    	}
    }
    else if ( strcmp((const char *)name, "property") == 0 )
    {
    	/*
    	 * Must be in element <configuration>, suppose this is guaranteed by
    	 * hawq control scripts.
    	 */
    	Assert(countConfiguration == 1);
    	if ( xmlDepth == 1 && xmlType  == XML_READER_TYPE_ELEMENT )
    	{
    		countProperty++;
    	}
    	else if ( xmlDepth == 1 && xmlType == XML_READER_TYPE_END_ELEMENT )
    	{
    		/*
    		 * In property element, there is only one <name> element, one <value>
    		 * element as a pair.
    		 */
    		if ( countProperty != 1 || countName != 1 || countValue != 1 )
    		{
    			elog(WARNING, "Element <property> is not correctly defined.");
				resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_PROPERTY;
				return;
    		}

    		/* Clear counters */
    		countProperty--;
    		countName--;
    		countValue--;
            if ( countDesc == 1 )
            {
    		    countDesc--;
            }

    		/* Build property item here. */
    		newproperty = (KVProperty) rm_palloc0(context, sizeof(KVPropertyData));
    		initSimpleStringWithContent(&(newproperty->Key),
    									context,
    									currentNameString.Str,
    									currentNameString.Len);
    		initSimpleStringWithContent(&(newproperty->Val),
    									context,
    									currentValueString.Str,
    									currentValueString.Len);
    		MEMORY_CONTEXT_SWITCH_TO(context)
    		*properties = lappend(*properties, newproperty);
    		MEMORY_CONTEXT_SWITCH_BACK

    		elog(LOG, "NOTE: Recognized configuration: %s=%s\n",
    				  newproperty->Key.Str,
    				  newproperty->Val.Str );
    	}
    	else
    	{
    		/* Wrong level or wrong node type. */
    		resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_PROPERTY;
    		elog(WARNING, "Wrong element <property> is parsed.");
    		return;
    	}
    }
    else if ( strcmp((const char *)name, "name") == 0 )
    {
    	if ( countConfiguration != 1 || countProperty != 1 )
    	{
    		resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_NAME;
    		elog(WARNING, "Element <name> is not correctly defined.");
    		return;
    	}

    	if ( xmlDepth == 2 && xmlType  == XML_READER_TYPE_ELEMENT )
    	{
    	    countName++;
		}
		else if ( xmlDepth == 2 && xmlType == XML_READER_TYPE_END_ELEMENT )
		{
			/* Do nothing here. */
		}
		else
		{
			/* Wrong level or wrong node type. */
			resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_NAME;
			elog(WARNING, "Wrong element <name> is parsed.");
			return;
		}

    }
    else if ( strcmp((const char *)name, "value") == 0 )
    {
    	if ( countConfiguration != 1 || countProperty != 1 || countName != 1)
    	{
    		resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_VALUE;
    		elog(WARNING, "Element <value> is not correctly defined.");
    		return;
    	}
    	if ( xmlDepth == 2 && xmlType  == XML_READER_TYPE_ELEMENT )
    	{
    	    countValue++;
		}
		else if ( xmlDepth == 2 && xmlType == XML_READER_TYPE_END_ELEMENT ) {
			/* Do nothing here. */
		}
		else
		{
			/* Wrong level or wrong node type. */
			resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_VALUE;
			elog(WARNING, "Wrong element <value> is parsed.");
			return;
		}
    }
    else if ( strcmp((const char *)name, "description") == 0 )
    {
    	if ( countConfiguration != 1 || countProperty != 1 )
    	{
    		resultCode = UTIL_PROPERTIES_ELEMENT_WRONG_DESC;
    		elog(WARNING, "Element <description> is not correctly defined.");
    		return;
    	}

    	if ( xmlDepth == 2 && xmlType  == XML_READER_TYPE_ELEMENT )
    	{
    	    countDesc++;
		}
    }
    else if ( xmlDepth == 3 &&
    		  xmlType == XML_READER_TYPE_TEXT &&
    		  value != NULL ) {
    	if ( countName == 1 && countValue == 0 && countDesc == 0 )
    	{
    		/* This is the name string. Save it. */
    		setSimpleStringWithContent( &currentNameString,
    									(char *)value,
    									xmlStrlen(value));
    	}
    	else if ( countValue == 1 && countDesc == 0 )
    	{
    		/* This is the value string. Save it. */
    		setSimpleStringWithContent( &currentValueString,
    		    						(char *)value,
    		    						xmlStrlen(value));
    	}
    	else if ( countDesc == 1 )
    	{
    		elog(LOG, "Recognized description string :%s\n", value);
    	}
    }
}


int processXMLPropertyFile(const char   *filename,
						   MCTYPE 		 context,
						   List 	   **properties)
{
	int				 res 	  	= FUNC_RETURN_OK;
	xmlTextReaderPtr reader		= NULL;
	int 			 readerres 	= 0;

	/* Create XML reader. */
	reader = xmlReaderForFile(filename, NULL, XML_PARSE_NOENT);
	if ( reader == NULL )
	{
		return UTIL_PROPERTIES_NO_FILE;
	}

	/* Set global variables to get ready for xml parsing. */
	countConfiguration 	= 0;
	countProperty 		= 0;
	countName			= 0;
	countValue			= 0;
	countDesc			= 0;
	resultCode			= FUNC_RETURN_OK;
	posNodeLine			= -1;
	posNodeColumn		= -1;

	initSimpleString(&currentNameString, context);
	initSimpleString(&currentValueString, context);

	readerres = xmlTextReaderRead(reader);
	while (readerres == 1 && resultCode == FUNC_RETURN_OK)
	{
		processXMLNode(reader, context, properties);
		readerres = xmlTextReaderRead(reader);
	}

	if ( resultCode != FUNC_RETURN_OK )
	{
		res = UTIL_PROPERTIES_INVALID_XML;
		goto exit;
	}

	readerres = xmlTextReaderIsValid(reader);
	if ( !(countConfiguration == 0 &&
		   countProperty == 0 &&
		   countName == 0 &&
		   countValue == 0) ||
		 readerres < 0 )
	{
		res = UTIL_PROPERTIES_INVALID_XML;
		goto exit;
	}

exit:
	if ( res != FUNC_RETURN_OK )
	{
		/* Clean up built objects in this function. */
		cleanPropertyList(context, properties);
	}

	freeSimpleStringContent(&currentNameString);
	freeSimpleStringContent(&currentValueString);

	xmlFreeTextReader(reader);

	xmlCleanupParser();

	return res;
}

void cleanPropertyList(MCTYPE context, List **properties)
{
	while( list_length(*properties) > 0 )
	{
		KVProperty property = lfirst(list_head(*properties));
		MEMORY_CONTEXT_SWITCH_TO(context)
		*properties = list_delete_first(*properties);
		MEMORY_CONTEXT_SWITCH_BACK

		freeSimpleStringContent(&(property->Key));
		freeSimpleStringContent(&(property->Val));
		rm_pfree(context, property);
	}
	Assert(*properties == NULL);
}

int findPropertyValue(List *properties, const char *key, SimpStringPtr *value)
{
	ListCell *cell = NULL;
	foreach(cell, properties)
	{
		KVProperty property = lfirst(cell);
		if ( SimpleStringComp(&(property->Key), (char *)key) == 0 )
		{
			(*value) = &(property->Val);
			return FUNC_RETURN_OK;
		}
	}

	*value = NULL;
	return UTIL_PROPERTIES_NO_KEY;
}

int  PropertyKeySubstring( SimpStringPtr 	key,
						   int 				index,
						   char 		  **start,
						   int 			   *length)
{
	int delcounter = 0;
	int scanner = 0;
	while( delcounter < index && scanner < key->Len ) {
		if ( key->Str[scanner] == '.' )
			delcounter++;
		scanner++;
	}
	if ( scanner >= key->Len ) {
		return FUNC_RETURN_FAIL;
	}

	*start = &(key->Str[scanner]);

	*length = 0;
	while( scanner < key->Len && key->Str[scanner] != '.' ) {
		(*length)++;
		scanner++;
	}

	return FUNC_RETURN_OK;
}

KVProperty createPropertyEmpty(MCTYPE context)
{
	KVProperty result = (KVProperty)rm_palloc0(context, sizeof(KVPropertyData));
	initSimpleString(&(result->Key), context);
	initSimpleString(&(result->Val), context);
	return result;
}

/**
 * Build a string formed as [tag1].[tag2].[index]
 */
void buildDottedPropertyNameString(SimpStringPtr 	 string,
								   const char 		*tag1,
								   const char 		*tag2,
								   int  			*index)
{
	static char indexstr[64];
	static char dot    = '.';
	static char endtag = '\0';

	SelfMaintainBufferData buff;

	initializeSelfMaintainBuffer(&buff, string->Context);
	if( tag1 != NULL ) {
		appendSelfMaintainBuffer(&buff, (char *)tag1, strlen(tag1));
	}
	if ( tag2 != NULL ) {
		if ( buff.Cursor>=0 ) {
			appendSelfMaintainBuffer(&buff, &dot, 1);
		}
		appendSelfMaintainBuffer(&buff, (char *)tag2, strlen(tag2));
	}
	if ( index != NULL ) {
		if ( buff.Cursor>=0 ) {
			appendSelfMaintainBuffer(&buff, &dot, 1);
		}
		sprintf(indexstr, "%d", *index);
		appendSelfMaintainBuffer(&buff, indexstr, strlen(indexstr));
	}
	appendSelfMaintainBuffer(&buff, &endtag, 1);

	setSimpleStringNoLen(string, buff.Buffer);
	destroySelfMaintainBuffer(&buff);

}

/**
 * Convert to a kvproperty instance as <tag1>.<tag2>.[index]=value
 */
KVProperty createPropertyOID(MCTYPE  		 context,
							 const char 	*tag1,
							 const char 	*tag2,
							 int			*index,
							 Oid	 		 value)
{
	KVProperty result = createPropertyEmpty(context);
	buildDottedPropertyNameString(&(result->Key), tag1, tag2, index);
	SimpleStringSetOid(&(result->Val), value);
	return result;
}

KVProperty createPropertyName(MCTYPE  		 context,
							  const char 	*tag1,
							  const char 	*tag2,
							  int			*index,
							  Name	  		 value)
{
	KVProperty result = createPropertyEmpty(context);
	buildDottedPropertyNameString(&(result->Key), tag1, tag2, index);
	SimpleStringSetName(&(result->Val), value);
	return result;
}

KVProperty createPropertyInt8(MCTYPE  		  context,
							  const char 	 *tag1,
							  const char 	 *tag2,
							  int	 		 *index,
							  int8_t  		  value)
{
	KVProperty result = createPropertyEmpty(context);
	buildDottedPropertyNameString(&(result->Key), tag1, tag2, index);
	SimpleStringSetInt8(&(result->Val), value);
	return result;
}

KVProperty createPropertyInt32(MCTYPE  		  context,
							   const char 	 *tag1,
							   const char 	 *tag2,
							   int	 		 *index,
							   int32_t  	  value)
{
	KVProperty result = createPropertyEmpty(context);
	buildDottedPropertyNameString(&(result->Key), tag1, tag2, index);
	SimpleStringSetInt32(&(result->Val), value);
	return result;
}

KVProperty createPropertyBool(MCTYPE  		context,
							  const char   *tag1,
							  const char   *tag2,
							  int	 	   *index,
							  Oid	  		value)
{
	KVProperty result = createPropertyEmpty(context);
	buildDottedPropertyNameString(&(result->Key), tag1, tag2, index);
	SimpleStringSetBool(&(result->Val), value);
	return result;
}

KVProperty createPropertyString(MCTYPE  	  context,
							    const char   *tag1,
							    const char   *tag2,
							    int	 	     *index,
							    const char   *value)
{
	KVProperty result = createPropertyEmpty(context);
	buildDottedPropertyNameString(&(result->Key), tag1, tag2, index);
	setSimpleStringNoLen(&(result->Val), (char *)value);
	return result;
}

KVProperty createPropertyFloat(MCTYPE		 context,
							   const char	*tag1,
							   const char   *tag2,
							   int          *index,
							   float         value)
{
	KVProperty result = createPropertyEmpty(context);
	buildDottedPropertyNameString(&(result->Key), tag1, tag2, index);
	SimpleStringSetFloat(&(result->Val), value);
	return result;
}
