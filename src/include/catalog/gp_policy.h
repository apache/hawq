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
 * gp_policy.h
 *	  definitions for the gp_distribution_policy catalog table
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#ifndef _GP_POLICY_H_
#define _GP_POLICY_H_

#include "access/attnum.h"
#include "catalog/genbki.h"
/*
 * Defines for gp_policy
 */
#define GpPolicyRelationName		"gp_distribution_policy"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_distribution_policy
   with (camelcase=GpPolicy, oid=false, relid=5002, content=MASTER_ONLY)
   (
   localoid  oid,
   bucketnum integer,
   attrnums  smallint[]
   );

   create unique index on gp_distribution_policy(localoid) with (indexid=6103, CamelCase=GpPolicyLocalOid);

   alter table gp_distribution_policy add fk localoid on pg_class(oid);

   TIDYCAT_ENDFAKEDEF
*/

#define GpPolicyRelationId  5002

CATALOG(gp_distribution_policy,5002) BKI_WITHOUT_OIDS
{
	Oid			localoid;
	int4		bucketnum;
	int2		attrnums[1];
} FormData_gp_policy;

#define Natts_gp_policy			3
#define Anum_gp_policy_localoid	  1
#define Anum_gp_policy_bucketnum  2
#define Anum_gp_policy_attrnums	  3

/*
 * GpPolicyType represents a type of policy under which a relation's
 * tuples may be assigned to a component database.
 */
typedef enum GpPolicyType
{
	POLICYTYPE_UNDEFINED,
	POLICYTYPE_PARTITIONED,		/* Tuples partitioned onto segment database. */
	POLICYTYPE_ENTRY			/* Tuples stored on enty database. */
} GpPolicyType;

/*
 * GpPolicy represents a Greenplum DB data distribution policy. The ptype field
 * is always significant.  Other fields may be specific to a particular
 * type.
 *
 * A GpPolicy is typically palloc'd with space for nattrs integer
 * attribute numbers (attrs) in addition to sizeof(GpPolicy).
 */
typedef
struct GpPolicy
{
	GpPolicyType ptype;

	int bucketnum;
	/* These fields apply to POLICYTYPE_PARTITIONED. */
	int			nattrs; // random: nattrs=0
	AttrNumber	attrs[1];		/* the first of nattrs attribute numbers.  */
} GpPolicy;

/*
 * GpPolicyCopy -- Return a copy of a GpPolicy object.
 *
 * The copy is palloc'ed in the specified context.
 */
GpPolicy *
GpPolicyCopy(MemoryContext mcxt, const GpPolicy *src);

/* GpPolicyEqual
 *
 * A field-by-field comparison just to facilitate comparing IntoClause
 * (which embeds this) in equalFuncs.c
 */
bool
GpPolicyEqual(const GpPolicy *lft, const GpPolicy *rgt);

/*
 * GpPolicyFetch
 *
 * Looks up a given Oid in the gp_distribution_policy table.
 * If found, returns an GpPolicy object (palloc'd in the specified
 * context) containing the info from the gp_distribution_policy row.
 * Else returns NULL.
 *
 * The caller is responsible for passing in a valid relation oid.  This
 * function does not check and assigns a policy of type POLICYTYPE_ENTRY
 * for any oid not found in gp_distribution_policy.
 */
GpPolicy *
GpPolicyFetch(MemoryContext mcxt, Oid tbloid);

/*
 * GpPolicyStore: sets the GpPolicy for a table.
 */
void
GpPolicyStore(Oid tbloid, const GpPolicy *policy);

void
GpPolicyReplace(Oid tbloid, const GpPolicy *policy);

void
GpPolicyRemove(Oid tbloid);


#endif /*_GP_POLICY_H_*/
