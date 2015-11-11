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
 * gp_global_sequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_GLOBAL_SEQUENCE_H
#define GP_GLOBAL_SEQUENCE_H

#define int8 int64

/*
 * Defines for gp_global_sequence table
 */
#define GpGlobalSequenceRelationName	"gp_global_sequence"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_global_sequence
   with (camelcase=GpGlobalSequence, oid=false, relid=5096, reltype_oid=6995, content=PERSISTENT)
   (
   sequence_num bigint
   );

   TIDYCAT_ENDFAKEDEF
*/

#define GpGlobalSequenceRelationId 5096

CATALOG(gp_global_sequence,5096) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	int8		sequence_num;
} FormData_gp_global_sequence;

#define Natts_gp_global_sequence				    1
#define Anum_gp_global_sequence_sequence_num     	1
 
typedef FormData_gp_global_sequence *Form_gp_global_sequence;


/*
 * gp_global_sequence table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_gp_global_sequence \
{ GpGlobalSequenceRelationId, {"sequence_num"},	20, -1, 8, 1, 0, -1, -1, true, 'p', 'd', true, false, false, true, 0 }

/*
 * gp_global_sequence table values for FormData_pg_class.
 */
#define Class_gp_global_sequence \
  {"gp_global_sequence"}, PG_CATALOG_NAMESPACE, GP_GLOBAL_SEQUENCE_RELTYPE_OID, BOOTSTRAP_SUPERUSERID, 0, \
               GpGlobalSequenceRelationId, GLOBALTABLESPACE_OID, \
               25, 10000, 0, 0, 0, 0, false, true, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_gp_global_sequence, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}


#undef int8

/*
 * The assigned sequence counters.
 */
typedef enum GpGlobalSequence
{
	GpGlobalSequence_PersistentRelfile = 1,	// Must start at 1, since tuple item of 0 are invalid.
	GpGlobalSequence_PersistentRelation = 2,
	GpGlobalSequence_PersistentDatabase = 3,
	GpGlobalSequence_PersistentTablespace = 4,
	GpGlobalSequence_PersistentFilespace = 5,
} GpGlobalSequence;

/*
 * This is the number of rows that the gp_global_sequence sequence table
 * should have, they should be a contiguous set of tids from (0,1)-(0,N)
 *
 * This is populated during the gp_persistent_build_db() function rather
 * than as part of normal dbinit operations.
 */
#define GpGlobalSequence_MaxSequenceTid 15

extern void GpGlobalSequence_GetValues(
	Datum						*values,

	int64						*sequenceNum);

extern void GpGlobalSequence_SetDatumValues(
	Datum					*values,

	int64					sequenceNum);

#endif   /* GP_GLOBAL_SEQUENCE_H */
