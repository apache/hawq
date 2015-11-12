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
/*-------------------------------------------------------------------------
 *
 * catcore.h
 *	  declarations for catcore access methods
 *
 * CatCore is for general use, but currently resides under ucs because
 * it is the only client that uses CatCore.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CATCORE_H
#define CATCORE_H

#include "postgres.h"
#include "access/attnum.h"

/* When change this, you should change it in generator script, too. */
#define MAX_SCAN_NUM 4

/*
 * Type in the catalog.
 * We denormalize btree procedures here for efficiency.
 */
typedef struct CatCoreType
{
	Oid			typid;			/* oid of this type */
	int16		typlen;			/* pg_type.typlen */
	bool		typbyval;		/* pg_type.typbyval */
	char		typalign;		/* pg_type.typalign */
	Oid			typelem;		/* pg_type.typelem */
	Oid			eqfunc;			/* function for BTEqualStrategyNumber */
	Oid			ltfunc;			/* function for BTLessStrategyNumber */
	Oid			lefunc;			/* function for BTLessEqualStrategyNumber */
	Oid			gefunc;			/* function for BTGreaterEqualStrategyNumber */
	Oid			gtfunc;			/* function for BTGreaterStrategyNumber */
} CatCoreType;

/*
 * Attribute in the catalog.
 */
typedef struct CatCoreAttr
{
	const char *attname;		/* attribute name */
	AttrNumber	attnum;			/* attribute number */
	const CatCoreType *atttyp;		/* attribute type */
} CatCoreAttr;

/*
 * Index in the catalog.
 * We use fixed-length array for key attributes because we know its maximum
 * length and this way is more compact than having separate pointers.
 */
typedef struct CatCoreIndex
{
	Oid			indexoid;		/* oid of this index relation */
	bool		unique;			/* is unique index? */
	/* key attributes.  filled with InvalidOid */
	AttrNumber	attnums[MAX_SCAN_NUM];
	int			nkeys;			/* number of valid attributes in attnums */
	int			syscacheid;		/* syscache id if available.  -1 otherwise */
} CatCoreIndex;

typedef struct CatCoreFKey
{
	AttrNumber	attnums[MAX_SCAN_NUM];
	int			nkeys;
	const char *refrelname;
	const CatCoreIndex *refindex;
} CatCoreFKey;

/*
 * Relation in the catalog.
 * This is only for tables, and index relations are not included.
 */
typedef struct CatCoreRelation
{
	const char *relname;		/* relation name */
	Oid			relid;			/* oid of this relation */
	const CatCoreAttr *attributes;	/* pointer to attribute array */
	int			natts;			/* number of attributes */
	const CatCoreIndex *indexes;/* pointer to index array */
	int			nindexes;		/* number of indexes */
	const CatCoreFKey *fkeys;	/* pointer to foreign key array */
	int			nfkeys;			/* number of fkeys */
	bool		hasoid;			/* true if this table has oid column */
} CatCoreRelation;

/*
 * catcoretable.c
 * These are generated from catcore json.
 */
extern const CatCoreAttr TableOidAttr;
extern const CatCoreRelation CatCoreRelations[];
extern const int CatCoreRelationSize;
extern const CatCoreType CatCoreTypes[];
extern const int CatCoreTypeSize;

/* catcore.c */
extern const CatCoreRelation *catcore_lookup_rel(char *name);
extern AttrNumber catcore_lookup_attnum(const CatCoreRelation *relation,
			char *attname);
extern const CatCoreAttr *catcore_lookup_attr(const CatCoreRelation *relation,
			char *attname);
extern const CatCoreType *catcore_lookup_type(Oid typid);


#endif
