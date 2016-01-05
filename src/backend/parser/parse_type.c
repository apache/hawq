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
 * parse_type.c
 *		handle type operations for parser
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/parser/parse_type.c,v 1.85 2006/10/04 00:29:56 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/catquery.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * LookupTypeName
 *		Given a TypeName object, get the OID of the referenced type.
 *		Returns InvalidOid if no such type can be found.
 *
 * NB: even if the returned OID is not InvalidOid, the type might be
 * just a shell.  Caller should check typisdefined before using the type.
 *
 * pstate is only used for error location info, and may be NULL.
 */
Oid
LookupTypeName(ParseState *pstate, const TypeName *typname)
{
	Oid			restype;

	/* Easy if it's an internally generated TypeName */
	if (typname->names == NIL)
		return typname->typid;

	if (typname->pct_type)
	{
		/* Handle %TYPE reference to type of an existing field */
		RangeVar   *rel = makeRangeVar(NULL /*catalogname*/, NULL, NULL, typname->location);
		char	   *field = NULL;
		Oid			relid;
		AttrNumber	attnum;

		/* deconstruct the name list */
		switch (list_length(typname->names))
		{
			case 1:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("improper %%TYPE reference (too few dotted names): %s",
					   NameListToString(typname->names)),
						 parser_errposition(pstate, typname->location)));
				break;
			case 2:
				rel->relname = strVal(linitial(typname->names));
				field = strVal(lsecond(typname->names));
				break;
			case 3:
				rel->schemaname = strVal(linitial(typname->names));
				rel->relname = strVal(lsecond(typname->names));
				field = strVal(lthird(typname->names));
				break;
			case 4:
				rel->catalogname = strVal(linitial(typname->names));
				rel->schemaname = strVal(lsecond(typname->names));
				rel->relname = strVal(lthird(typname->names));
				field = strVal(lfourth(typname->names));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("improper %%TYPE reference (too many dotted names): %s",
								NameListToString(typname->names)),
						 parser_errposition(pstate, typname->location)));
				break;
		}

		/* look up the field */
		relid = RangeVarGetRelid(rel, false, false /*allowHcatalog*/);
		attnum = get_attnum(relid, field);
		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							field, rel->relname),
					 errOmitLocation(true),
					 parser_errposition(pstate, typname->location)));
		restype = get_atttype(relid, attnum);

		/* this construct should never have an array indicator */
		Assert(typname->arrayBounds == NIL);

		/* emit nuisance notice */
		ereport(NOTICE,
				(errmsg("type reference %s converted to %s",
						TypeNameToString(typname),
						format_type_be(restype))));
	}
	else
	{
		/* Normal reference to a type name */
		char	   *schemaname;
		char	   *tname;

		/* deconstruct the name list */
		DeconstructQualifiedName(typname->names, &schemaname, &tname);

		/* If an array reference, look up the array type instead */
		if (typname->arrayBounds != NIL)
			tname = makeArrayTypeName(tname);

		if (schemaname)
		{
			/* Look in specific schema only */
			Oid			namespaceId;

			namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);

			restype = caql_getoid(
					NULL,
					cql("SELECT oid FROM pg_type "
						" WHERE typname = :1 "
						" AND typnamespace = :2 ",
						PointerGetDatum(tname),
						ObjectIdGetDatum(namespaceId)));
		}
		else
		{
			/* Unqualified type name, so search the search path */
			restype = TypenameGetTypid(tname);
		}
	}

	return restype;
}

/*
 * appendTypeNameToBuffer
 *		Append a string representing the name of a TypeName to a StringInfo.
 *		This is the shared guts of TypeNameToString and TypeNameListToString.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
static void
appendTypeNameToBuffer(const TypeName *typname, StringInfo string)
{
	if (typname->names != NIL)
	{
		/* Emit possibly-qualified name as-is */
		ListCell   *l;

		foreach(l, typname->names)
		{
			if (l != list_head(typname->names))
				appendStringInfoChar(string, '.');
			appendStringInfoString(string, strVal(lfirst(l)));
		}
	}
	else
	{
		/* Look up internally-specified type */
		appendStringInfoString(string, format_type_be(typname->typid));
	}

	/*
	 * Add decoration as needed, but only for fields considered by
	 * LookupTypeName
	 */
	if (typname->pct_type)
		appendStringInfoString(string, "%TYPE");

	if (typname->arrayBounds != NIL)
		appendStringInfoString(string, "[]");
}

/*
 * TypeNameToString
 *		Produce a string representing the name of a TypeName.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
char *
TypeNameToString(const TypeName *typename)
{
	StringInfoData string;

	initStringInfo(&string);
	appendTypeNameToBuffer(typename, &string);
	return string.data;
}

/*
 * TypeNameListToString
 *		Produce a string representing the name(s) of a List of TypeNames
 */
char *
TypeNameListToString(List *typenames)
{
	StringInfoData string;
	ListCell   *l;

	initStringInfo(&string);
	foreach(l, typenames)
	{
		TypeName   *typename = (TypeName *) lfirst(l);

		Assert(IsA(typename, TypeName));
		if (l != list_head(typenames))
			appendStringInfoChar(&string, ',');
		appendTypeNameToBuffer(typename, &string);
	}
	return string.data;
}

/*
 * typenameTypeId - given a TypeName, return the type's OID
 *
 * This is equivalent to LookupTypeName, except that this will report
 * a suitable error message if the type cannot be found or is not defined.
 */
Oid
typenameTypeId(ParseState *pstate, const TypeName *typname)
{
	Oid			typoid;

	typoid = LookupTypeName(pstate, typname);
	if (!OidIsValid(typoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typname)),
				 errOmitLocation(true),
				 parser_errposition(pstate, typname->location)));

	if (!get_typisdefined(typoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typname)),
				 errOmitLocation(true),
				 parser_errposition(pstate, typname->location)));
	return typoid;
}

/*
 * typenameType - given a TypeName, return a Type structure
 *
 * This is equivalent to typenameTypeId + syscache fetch of Type tuple.
 * NB: caller must ReleaseType the type tuple when done with it.
 */
Type
typenameType(ParseState *pstate, const TypeName *typname)
{
	Oid			typoid;
	HeapTuple	tup;

	typoid = LookupTypeName(pstate, typname);
	if (!OidIsValid(typoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typname)),
				 errOmitLocation(true)));
	tup = SearchSysCache(TYPEOID,
						 ObjectIdGetDatum(typoid),
						 0, 0, 0);
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for type %u", typoid);
	if (!((Form_pg_type) GETSTRUCT(tup))->typisdefined)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typname)),
				 errOmitLocation(true),
				 parser_errposition(pstate, typname->location)));
	return (Type) tup;
}

/* return a Type structure, given a type id */
/* NB: caller must ReleaseType the type tuple when done with it */
Type
typeidType(Oid id)
{
	HeapTuple	tup;

	tup = SearchSysCache(TYPEOID,
						 ObjectIdGetDatum(id),
						 0, 0, 0);
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", id);
	return (Type) tup;
}

/* given type (as type struct), return the type OID */
Oid
typeTypeId(Type tp)
{
	if (tp == NULL)				/* probably useless */
		elog(ERROR, "typeTypeId() called with NULL type struct");
	return HeapTupleGetOid(tp);
}

/* given type (as type struct), return the length of type */
int16
typeLen(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typlen;
}

/* given type (as type struct), return the value of its 'byval' attribute.*/
bool
typeByVal(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typbyval;
}

/* given type (as type struct), return the value of its 'typtype' attribute.*/
char
typeTypType(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typtype;
}

/* given type (as type struct), return the name of type */
char *
typeTypeName(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	/* pstrdup here because result may need to outlive the syscache entry */
	return pstrdup(NameStr(typ->typname));
}

Oid
typeTypeRelid(Type typ)
{
	Form_pg_type typtup;

	typtup = (Form_pg_type) GETSTRUCT(typ);

	return typtup->typrelid;
}

/*
 * Given a type structure and a string, returns the internal representation
 * of that string.	The "string" can be NULL to perform conversion of a NULL
 * (which might result in failure, if the input function rejects NULLs).
 */
Datum
stringTypeDatum(Type tp, char *string, int32 atttypmod)
{
	Oid			typinput;
	Oid			typioparam;

	typinput = ((Form_pg_type) GETSTRUCT(tp))->typinput;
	typioparam = getTypeIOParam(tp);
	return OidInputFunctionCall(typinput, string,
								typioparam, atttypmod);
}

/* given a typeid, return the type's typrelid (associated relation, if any) */
Oid
typeidTypeRelid(Oid type_id)
{
	Oid			result;
	int			fetchCount = 0;

	result = caql_getoid_plus(
					NULL,
					&fetchCount,
					NULL,
					cql("SELECT typrelid FROM pg_type "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(type_id)));

	if (0 == fetchCount)
		elog(ERROR, "cache lookup failed for type %u", type_id);

	return result;
}

/*
 * error context callback for parse failure during parseTypeString()
 */
static void
pts_error_callback(void *arg)
{
	const char *str = (const char *) arg;

	errcontext("invalid type name \"%s\"", str);

	/*
	 * Currently we just suppress any syntax error position report, rather
	 * than transforming to an "internal query" error.	It's unlikely that a
	 * type name is complex enough to need positioning.
	 */
	errposition(0);
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and convert it to a type OID and type modifier.
 */
void
parseTypeString(const char *str, Oid *type_id, int32 *typmod)
{
	StringInfoData buf;
	List	   *raw_parsetree_list;
	SelectStmt *stmt;
	ResTarget  *restarget;
	TypeCast   *typecast;
	TypeName   *typname;
	ErrorContextCallback ptserrcontext;

	/* make sure we give useful error for empty input */
	if (strspn(str, " \t\n\r\f") == strlen(str))
		goto fail;

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT NULL::%s", str);

	/*
	 * Setup error traceback support in case of ereport() during parse
	 */
	ptserrcontext.callback = pts_error_callback;
	ptserrcontext.arg = (void *) str;
	ptserrcontext.previous = error_context_stack;
	error_context_stack = &ptserrcontext;

	raw_parsetree_list = raw_parser(buf.data);

	error_context_stack = ptserrcontext.previous;

	/*
	 * Make sure we got back exactly what we expected and no more; paranoia is
	 * justified since the string might contain anything.
	 */
	if (list_length(raw_parsetree_list) != 1)
		goto fail;
	stmt = (SelectStmt *) linitial(raw_parsetree_list);
	if (stmt == NULL ||
		!IsA(stmt, SelectStmt) ||
		stmt->distinctClause != NIL ||
		stmt->intoClause != NULL ||
		stmt->fromClause != NIL ||
		stmt->whereClause != NULL ||
		stmt->groupClause != NIL ||
		stmt->havingClause != NULL ||
		stmt->withClause != NULL ||
		stmt->valuesLists != NIL ||
		stmt->sortClause != NIL ||
		stmt->limitOffset != NULL ||
		stmt->limitCount != NULL ||
		stmt->lockingClause != NIL ||
		stmt->op != SETOP_NONE)
		goto fail;
	if (list_length(stmt->targetList) != 1)
		goto fail;
	restarget = (ResTarget *) linitial(stmt->targetList);
	if (restarget == NULL ||
		!IsA(restarget, ResTarget) ||
		restarget->name != NULL ||
		restarget->indirection != NIL)
		goto fail;
	typecast = (TypeCast *) restarget->val;
	if (typecast == NULL ||
		!IsA(typecast, TypeCast) ||
		typecast->arg == NULL ||
		!IsA(typecast->arg, A_Const))
		goto fail;
	typname = typecast->typname;
	if (typname == NULL ||
		!IsA(typname, TypeName))
		goto fail;
	if (typname->setof)
		goto fail;

	*type_id = typenameTypeId(NULL, typname);
	*typmod = typname->typmod;

	pfree(buf.data);

	return;

fail:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid type name \"%s\"", str)));
}
