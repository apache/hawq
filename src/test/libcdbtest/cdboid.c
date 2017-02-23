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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * cdboid.c
 *
 */
#include "postgres.h"

#include <assert.h>
#include <ctype.h>
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "cdboid.h"

/*
 * cdb_get_oid takes 0 arguments
 * and returns an int4
 */
PG_FUNCTION_INFO_V1(cdb_get_oid);
Datum
cdb_get_oid(PG_FUNCTION_ARGS)
{
	int result;

	if ( SPI_OK_CONNECT != SPI_connect() )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_connect failed in cdb_eget_oid" )));
	}

	if ( SPI_OK_UTILITY != SPI_execute( "CREATE TEMPORARY TABLE pgdump_oid (dummy integer) WITH OIDS", false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in cdb_get_oid" )));
	}

	if ( SPI_OK_INSERT != SPI_execute( "insert into pgdump_oid values(0)", false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed to insert a row into pgdump_oid in cdb_get_oid" )));
	}

	if ( SPI_OK_SELECT != SPI_execute( "select oid from pgdump_oid", false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in cdb_get_oid" )));
	}

	if ( SPI_processed == 0 )
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("No rows in pgdump_oid in cdb_get_oid" )));


	TupleDesc tupdesc = SPI_tuptable->tupdesc;
	result = atoi( SPI_getvalue( SPI_tuptable->vals[0], tupdesc, 1));

	if ( SPI_OK_UTILITY != SPI_execute( "DROP TABLE pgdump_oid", false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in cdb_get_oid" )));
	}

	SPI_finish();

	PG_RETURN_INT32(result);
}

/*
 * cdb_set_oid takes 1 argument (the new oid max value)
 * and returns a boolean
 */
PG_FUNCTION_INFO_V1(cdb_set_oid);
Datum
cdb_set_oid(PG_FUNCTION_ARGS)
{
	int32 maxoid = PG_GETARG_INT32(0);
	char *tempFileName = tempnam( NULL, "TMPCP" );

/*	elog(NOTICE, "tempFileName = %s", tempFileName ); */

	StringInfoData buffer;
	initStringInfo( &buffer );

	appendStringInfo( &buffer, "%u\t0\n\\.\n", maxoid );

	FILE *fptr = fopen(tempFileName, "w");
	if ( strlen(buffer.data) != fwrite( buffer.data, 1, strlen(buffer.data), fptr) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in in temp file write in cdb_set_oid" )));

	}

	fclose(fptr);
	pfree(buffer.data);
	buffer.data = NULL;

	if ( SPI_OK_CONNECT != SPI_connect() )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_connect failed in cdb_set_oid" )));
	}

	if ( SPI_OK_UTILITY != SPI_execute( "CREATE TEMPORARY TABLE pgdump_oid (dummy integer) WITH OIDS", false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in cdb_set_oid" )));
	}

	
	initStringInfo( &buffer );

	appendStringInfo( &buffer, "COPY pgdump_oid WITH OIDS FROM '%s'", tempFileName );
		
	if ( SPI_OK_UTILITY != SPI_execute( buffer.data, false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in copy command in cdb_set_oid" )));
	}

	remove( tempFileName );

	pfree(buffer.data);

	if ( SPI_OK_UTILITY != SPI_execute( "DROP TABLE pgdump_oid", false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in cdb_set_oid" )));
	}

	SPI_finish();

	PG_RETURN_BOOL(true);
}

