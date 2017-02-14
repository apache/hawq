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


#include "postgres.h"
#include "miscadmin.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/queue.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* tests for caql coverage in regproc.c */
extern Datum caql_bootstrap_regproc(PG_FUNCTION_ARGS);

/* tests for caql coverage in lsyscache.c */
extern Datum check_get_atttypmod(PG_FUNCTION_ARGS);
extern Datum check_get_opname(PG_FUNCTION_ARGS);
extern Datum check_get_typ_typrelid(PG_FUNCTION_ARGS);
extern Datum check_get_base_element_type(PG_FUNCTION_ARGS);

/* test for caql coverage in spi.c */
extern Datum check_SPI_gettype(PG_FUNCTION_ARGS);

/* tests for caql coverage in cdbutil.c */
extern Datum check_master_standby_dbid(PG_FUNCTION_ARGS);
extern Datum check_dbid_get_dbinfo(PG_FUNCTION_ARGS);
extern Datum check_contentid_get_dbid(PG_FUNCTION_ARGS);

/* tests for caql coverage in segadmin.c */
extern Datum check_gp_activate_standby(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(caql_bootstrap_regproc);
Datum
caql_bootstrap_regproc(PG_FUNCTION_ARGS)
{
	char	   *cstr_regprocin = "boolin";
	char	   *cstr_regoperin = "#>="; /* we should pick up a unique name */
	char	   *cstr_regclassin = "pg_class";
	char	   *cstr_regtypein = "bool";
	Datum		result;
	StringInfoData	buf;

	initStringInfo(&buf);

	SetProcessingMode(BootstrapProcessing);
	/* regproc */
	result = DirectFunctionCall1(regprocin, CStringGetDatum(cstr_regprocin));
	appendStringInfo(&buf, "regprocin(%s) = %d\n",
			cstr_regprocin, DatumGetObjectId(result));

	/* regoper */
	result = DirectFunctionCall1(regoperin, CStringGetDatum(cstr_regoperin));
	appendStringInfo(&buf, "regoperin(%s) = %d\n",
			cstr_regoperin, DatumGetObjectId(result));

	/* regclass */
	result = DirectFunctionCall1(regclassin, CStringGetDatum(cstr_regclassin));
	appendStringInfo(&buf, "regclassin(%s) = %d\n",
			cstr_regclassin, DatumGetObjectId(result));

	/* regtype */
	result = DirectFunctionCall1(regtypein, CStringGetDatum(cstr_regtypein));
	appendStringInfo(&buf, "regtypein(%s) = %d\n",
			cstr_regtypein, DatumGetObjectId(result));
	SetProcessingMode(NormalProcessing);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Testing caql coverage for lsyscache.c-
 */
PG_FUNCTION_INFO_V1(check_get_atttypmod);
Datum
check_get_atttypmod(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	AttrNumber attnum = PG_GETARG_INT16(1);
	int32 result;

	result = get_atttypmod(relid, attnum);

	PG_RETURN_INT32(result);
}

PG_FUNCTION_INFO_V1(check_get_opname);
Datum
check_get_opname(PG_FUNCTION_ARGS)
{
	Oid opno = PG_GETARG_OID(0);
	char *result;
	StringInfoData buf; 

	initStringInfo(&buf); 
	result = get_opname(opno);
	if (result)
		appendStringInfo(&buf, "%s", result);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

PG_FUNCTION_INFO_V1(check_get_typ_typrelid);
Datum
check_get_typ_typrelid(PG_FUNCTION_ARGS)
{
	Oid typid = PG_GETARG_OID(0);
	Oid result;

	result = get_typ_typrelid(typid);

	PG_RETURN_OID(result);
}

PG_FUNCTION_INFO_V1(check_get_base_element_type);
Datum
check_get_base_element_type(PG_FUNCTION_ARGS)
{
	Oid typid = PG_GETARG_OID(0);
	Oid result;

	result = get_base_element_type(typid);

	PG_RETURN_OID(result);
}

PG_FUNCTION_INFO_V1(check_SPI_gettype);
Datum
check_SPI_gettype(PG_FUNCTION_ARGS)
{
	int		fnumber = PG_GETARG_INT32(0);
	Relation	rel = relation_open(RelationRelationId, AccessShareLock);
	char		*name = SPI_gettype(RelationGetDescr(rel), fnumber);
	relation_close(rel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(name));
}

PG_FUNCTION_INFO_V1(check_gp_activate_standby);
Datum
check_gp_activate_standby(PG_FUNCTION_ARGS)
{
	/* Pretend as if I'm a standby.  The dbid is given as an argument. */
	GpIdentity.dbid = PG_GETARG_INT16(0);

	/* There is no DirectFunctionCall0 */
	DirectFunctionCall1(gp_activate_standby, Int32GetDatum(0));

	PG_RETURN_NULL();
}

