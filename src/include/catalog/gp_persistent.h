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
 * gp_persistent.h
 *
 * Global tables:
 *     gp_persistent_relation_node
 *     gp_persistent_database_node
 *     gp_persistent_tablespace_node
 *     gp_persistent_filespace_node
 *
 * Per database table:
 *     gp_relation_node
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_PERSISTENT_H
#define GP_PERSISTENT_H

#include "access/htup.h"
#include "access/tupdesc.h"
#include "storage/itemptr.h"
#include "access/persistentfilesysobjname.h"
#include "access/xlogdefs.h"
#include "catalog/indexing.h"
#include "catalog/gp_global_sequence.h"
#include "catalog/pg_shdepend.h"

#define int8 int64
#define tid ItemPointerData
#define gpxlogloc XLogRecPtr

/*
 * Defines for gp_persistent_relfile_node table
 */
#define GpPersistentRelfileNodeRelationName	"gp_persistent_relfile_node"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_persistent_relfile_node
   with (shared=true, oid=false, relid=5089, reltype_oid=6989, content=PERSISTENT)
   (
   tablespace_oid                                     oid       ,
   database_oid                                       oid       ,
   relfilenode_oid                                    oid       ,
   segment_file_num                                   integer   ,
   relation_storage_manager                           smallint  ,
   persistent_state                                   smallint  ,
   relation_bufpool_kind                              integer   ,
   parent_xid                                         integer   ,
   persistent_serial_num                              bigint    ,
   previous_free_tid                                  tid       ,
   );

   TIDYCAT_ENDFAKEDEF
*/

#define GpPersistentRelfileNodeRelationId 5089

// UNDONE: Change reserved column to relation_bufpool_kind in tidycat above and CATALOG
// UNDONE: defintition below.  The Anum_* DEFINE below has already been renamed.
CATALOG(gp_persistent_relfile_node,5089) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid			tablespace_oid;
	Oid			database_oid;
	Oid			relfilenode_oid;
	int4		segment_file_num;
	int2		relation_storage_manager;
	int2		persistent_state;
	int4		relation_bufpool_kind;
	int4		parent_xid;
	int8		persistent_serial_num;
	tid			previous_free_tid;
} FormData_gp_persistent_relfile_node;

#define Natts_gp_persistent_relfile_node				    				  					10
#define Anum_gp_persistent_relfile_node_tablespace_oid  					  					1
#define Anum_gp_persistent_relfile_node_database_oid   					  					2
#define Anum_gp_persistent_relfile_node_relfilenode_oid					  					3
#define Anum_gp_persistent_relfile_node_segment_file_num					  					4
#define Anum_gp_persistent_relfile_node_relation_storage_manager			  					5
#define Anum_gp_persistent_relfile_node_persistent_state					  					6
#define Anum_gp_persistent_relfile_node_relation_bufpool_kind									7
#define Anum_gp_persistent_relfile_node_parent_xid  						  					8
#define Anum_gp_persistent_relfile_node_persistent_serial_num     			  					9
#define Anum_gp_persistent_relfile_node_previous_free_tid  				  					10
 
typedef FormData_gp_persistent_relfile_node *Form_gp_persistent_relfile_node;


/*
 * gp_persistent_relfile_node table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_gp_persistent_relfile_node \
{ GpPersistentRelfileNodeRelationId, {"tablespace_oid"}, 										26, -1,	4, 1, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"database_oid"}, 										26, -1,	4, 2, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"relfilenode_oid"}, 										26, -1,	4, 3, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"segment_file_num"}, 									23, -1, 4, 4, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"relation_storage_manager"},								21, -1, 2, 5, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"persistent_state"}, 									21, -1, 2, 6, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"relation_bufpool_kind"},								23, -1, 4, 16, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"parent_xid"},											23, -1, 4, 17, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"persistent_serial_num"},								20, -1, 8, 18, 0, -1, -1, true, 'p', 'd', true, false, false, true, 0 }, \
{ GpPersistentRelfileNodeRelationId, {"previous_free_tid"},									27, -1, 6, 19, 0, -1, -1, false, 'p', 's', true, false, false, true, 0 }, \

/*
 * gp_persistent_relfile_node table values for FormData_pg_class.
 */
#define Class_gp_persistent_relfile_node \
  {"gp_persistent_relfile_node"}, PG_CATALOG_NAMESPACE, GP_PERSISTENT_RELFILE_NODE_OID, BOOTSTRAP_SUPERUSERID, 0, \
               GpPersistentRelfileNodeRelationId, GLOBALTABLESPACE_OID, \
               25, 10000, 0, 0, 0, 0, false, true, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_gp_persistent_relfile_node, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}



/*
 * Defines for gp_relfile_node table
 */
#define GpRelfileNodeRelationName		"gp_relfile_node"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_relfile_node
   with (shared=false, oid=false, relid=5094, reltype_oid=6994, content=PERSISTENT)
   (
   relfilenode_oid                               oid     ,
   segment_file_num                              integer ,
   persistent_tid                                tid     ,
   persistent_serial_num                         bigint  
   );

   alter table gp_relfile_node add fk relfilenode_oid on pg_class(oid);

   TIDYCAT_ENDFAKEDEF
*/


#define GpRelfileNodeRelationId 5094

CATALOG(gp_relfile_node,5094) BKI_WITHOUT_OIDS
{
	Oid			relfilenode_oid;
	int4		segment_file_num;
	tid			persistent_tid;
	int8		persistent_serial_num;
} FormData_gp_relfile_node;

#define Natts_gp_relfile_node				    							4
#define Anum_gp_relfile_node_relfilenode_oid								1
#define Anum_gp_relfile_node_segment_file_num								2
#define Anum_gp_relfile_node_persistent_tid      							3
#define Anum_gp_relfile_node_persistent_serial_num     						4
 
typedef FormData_gp_relfile_node *Form_gp_relfile_node;

/*
 * gp_relfile_node table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_gp_relfile_node \
{ GpRelfileNodeRelationId, {"relfilenode_oid"}, 									26, -1,	4, 1, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpRelfileNodeRelationId, {"segment_file_num"}, 									23, -1, 4, 2, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpRelfileNodeRelationId, {"persistent_tid"},										27, -1, 6, 4, 0, -1, -1, false, 'p', 's', true, false, false, true, 0 }, \
{ GpRelfileNodeRelationId, {"persistent_serial_num"},								20, -1, 8, 5, 0, -1, -1, true, 'p', 'd', true, false, false, true, 0 }

/*
 * gp_relfile_node table values for FormData_pg_class.
 */
#define Class_gp_relfile_node \
  {"gp_relfile_node"}, PG_CATALOG_NAMESPACE, GP_RELFILE_NODE_OID, BOOTSTRAP_SUPERUSERID, 0, \
               GpRelfileNodeRelationId, DEFAULTTABLESPACE_OID, \
               25, 10000, 0, 0, 0, 0, true, false, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_gp_relfile_node, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}

/*
 * gp_relfile_node's index.
 */
#define Natts_gp_relfile_node_index				    					2
 
/*
 * gp_relfile_node_index table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_gp_relfile_node_index \
{ GpRelfileNodeRelationId, {"relfilenode_oid"}, 	26, -1,	4, 1, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ GpRelfileNodeRelationId, {"segment_file_num"}, 	23, -1, 4, 2, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }

/*
 * gp_relfile_node_index index values for FormData_pg_class.
 */
#define Class_gp_relfile_node_index \
  {"gp_relfile_node_index"}, PG_CATALOG_NAMESPACE, 0, BOOTSTRAP_SUPERUSERID, BTREE_AM_OID, \
               GpRelfileNodeOidIndexId, DEFAULTTABLESPACE_OID, \
               25, 10000, 0, 0, 0, 0, false, false, RELKIND_INDEX, RELSTORAGE_HEAP, Natts_gp_relfile_node_index, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}

/*
 * gp_relfile_node_index AM values for FormData_pg_am.
 */
#define Am_gp_relfile_node_index \
   Am_btree

/*
 * Init fields of int2vector with 0 or other values.  Needs to be populated for real later.
 * vl_len_, ndim, dataoffset, elemtype, dim1, lbound1, values.
 */
#define Init_int2vector 0, 1, 0, InvalidOid, 0, 0, {0}

#define Init_oidvector 0, 1, 0, InvalidOid, 0, 0, {InvalidOid}

#define Init_text {'\0','\0','\0','\0'},{'\0'}
/*
 * gp_relfile_node_index Index values for FormData_pg_index.
 */
#define Index_gp_relfile_node_index \
   GpRelfileNodeOidIndexId, GpRelfileNodeRelationId, Natts_gp_relfile_node_index, true, false, false, true, {Init_int2vector}, {Init_oidvector}, {Init_text}, {Init_text}

#define IndKey_gp_relfile_node_index \
    1, 2

#define IndClass_gp_relfile_node_index \
    OID_BTREE_OPS_OID, INT4_BTREE_OPS_OID

/*
 * Defines for gp_persistent_relation_node table
 */
#define GpPersistentRelatioinNodeRelationName "gp_persistent_relation_node"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_persistent_relation_node
   with (shared=true, oid=false, relid=5090, reltype_oid=6990, content=PERSISTENT)
   (
   tablespace_oid									oid			,
   database_oid										oid			,
   relfilenode_oid									oid			,
   persistent_state									smallint		,
   reserved											integer		,
   parent_xid										integer		,
   persistent_serial_num								bigint		,
   previous_free_tid									tid			,
   );
   TIDYCAT_BEGINFAKEDEF
*/

#define GpPersistentRelationNodeRelationId 5090

CATALOG(gp_persistent_relation_node,5090) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid		tablespace_oid;
	Oid		database_oid;
	Oid		relfilenode_oid;
	int2		persistent_state;
	int4		reserved;
	int4		parent_xid;
	int8		persistent_serial_num;
	tid		previous_free_tid;
} FormData_gp_persistent_relation_node;

#define Natts_gp_persistent_relation_node												8
#define Anum_gp_persistent_relation_node_tablespace_oid									1
#define Anum_gp_persistent_relation_node_database_oid									2
#define Anum_gp_persistent_relation_node_relfilenode_oid									3
#define Anum_gp_persistent_relation_node_persistent_state									4
#define Anum_gp_persistent_relation_node_reserved										5
#define Anum_gp_persistent_relation_node_parent_xid										6
#define Anum_gp_persistent_relation_node_persistent_serial_num							7
#define Anum_gp_persistent_relation_node_previous_free_tid								8

typedef FormData_gp_persistent_relation_node *Form_gp_persistent_relation_node;

/*
 * gp_persistent_relation_node table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_gp_persistent_relation_node \
{ GpPersistentRelationNodeRelationId, {"tablespace_oid"}, 									26, -1, 4, 2, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelationNodeRelationId, {"database_oid"}, 										26, -1, 4, 3, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelationNodeRelationId, {"relfilenode_oid"}, 									26, -1, 4, 4, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelationNodeRelationId, {"persistent_state"}, 									21, -1, 2, 5, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ GpPersistentRelationNodeRelationId, {"reserved"},											23, -1, 4, 8, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelationNodeRelationId, {"parent_xid"},										23, -1, 4, 9, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentRelationNodeRelationId, {"persistent_serial_num"},								20, -1, 8, 10, 0, -1, -1, true, 'p', 'd', true, false, false, true, 0 }, \
{ GpPersistentRelationNodeRelationId, {"previous_free_tid"},									27, -1, 6, 11, 0, -1, -1, false, 'p', 's', true, false, false, true, 0 }, \

/*
 * gp_persistent_relation_node table values for FormData_pg_class.
 */
#define Class_gp_persistent_relation_node \
  {"gp_persistent_relation_node"}, PG_CATALOG_NAMESPACE, GP_PERSISTENT_RELATION_NODE_OID, BOOTSTRAP_SUPERUSERID, 0, \
               GpPersistentRelationNodeRelationId, GLOBALTABLESPACE_OID, \
               2, 100, 0, 0, 0, 0, false, true, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_gp_persistent_relation_node, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}

/*
 * Defines for gp_persistent_database_node table
 */
#define GpPersistentDatabaseNodeRelationName		"gp_persistent_database_node"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_persistent_database_node
   with (shared=true, oid=false, relid=5091, reltype_oid=6991, content=PERSISTENT)
   (
   tablespace_oid                                oid      ,
   database_oid                                  oid      ,
   persistent_state                              smallint ,
   reserved                                      integer  ,
   parent_xid                                    integer  ,
   persistent_serial_num                         bigint   ,
   previous_free_tid                             tid      ,
   );

   TIDYCAT_ENDFAKEDEF
*/

#define GpPersistentDatabaseNodeRelationId 5091

CATALOG(gp_persistent_database_node,5091) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid			tablespace_oid;
	Oid			database_oid;
	int2		persistent_state;
	int4		reserved;
	int4		parent_xid;
	int8		persistent_serial_num;
	tid			previous_free_tid;
} FormData_gp_persistent_database_node;

#define Natts_gp_persistent_database_node				    							7
#define Anum_gp_persistent_database_node_tablespace_oid  								1
#define Anum_gp_persistent_database_node_database_oid   								2
#define Anum_gp_persistent_database_node_persistent_state      							3
#define Anum_gp_persistent_database_node_reserved  										4
#define Anum_gp_persistent_database_node_parent_xid  									5
#define Anum_gp_persistent_database_node_persistent_serial_num  						6
#define Anum_gp_persistent_database_node_previous_free_tid	  							7

typedef FormData_gp_persistent_database_node *Form_gp_persistent_database_node;


/*
 * gp_persistent_database_node table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_gp_persistent_database_node \
{ GpPersistentDatabaseNodeRelationId, {"tablespace_oid"}, 									26, -1,	4, 2, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentDatabaseNodeRelationId, {"database_oid"}, 									26, -1,	4, 3, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentDatabaseNodeRelationId, {"persistent_state"}, 								21, -1, 2, 4, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ GpPersistentDatabaseNodeRelationId, {"reserved"},											23, -1, 4, 7, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentDatabaseNodeRelationId, {"parent_xid"},										23, -1, 4, 8, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentDatabaseNodeRelationId, {"persistent_serial_num"},							20, -1, 8, 9, 0, -1, -1, true, 'p', 'd', true, false, false, true, 0 }, \
{ GpPersistentDatabaseNodeRelationId, {"previous_free_tid"},								27, -1, 6, 10, 0, -1, -1, false, 'p', 's', true, false, false, true, 0 }, \

/*
 * gp_persistent_database_node table values for FormData_pg_class.
 */
#define Class_gp_persistent_database_node \
  {"gp_persistent_database_node"}, PG_CATALOG_NAMESPACE, GP_PERSISTENT_DATABASE_NODE_OID, BOOTSTRAP_SUPERUSERID, 0, \
               GpPersistentDatabaseNodeRelationId, GLOBALTABLESPACE_OID, \
               2, 100, 0, 0, 0, 0, false, true, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_gp_persistent_database_node, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}


/*
 * Defines for gp_persistent_tablespace_node table
 */
#define GpPersistentTablespaceNodeRelationName		"gp_persistent_tablespace_node"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_persistent_tablespace_node
   with (shared=true, oid=false, relid=5092, reltype_oid=6992, content=PERSISTENT)
   (
   filespace_oid                                 oid      ,
   tablespace_oid                                oid      ,
   persistent_state                              smallint ,
   reserved                                      integer  ,
   parent_xid                                    integer  ,
   persistent_serial_num                         bigint   ,
   previous_free_tid                             tid      ,
   );

   TIDYCAT_ENDFAKEDEF
*/


#define GpPersistentTablespaceNodeRelationId 5092

CATALOG(gp_persistent_tablespace_node,5092) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid			filespace_oid;
	Oid			tablespace_oid;
	int2		persistent_state;
	int4		reserved;
	int4		parent_xid;
	int8		persistent_serial_num;
	tid			previous_free_tid;
} FormData_gp_persistent_tablespace_node;

#define Natts_gp_persistent_tablespace_node				    								7
#define Anum_gp_persistent_tablespace_node_filespace_oid  									1
#define Anum_gp_persistent_tablespace_node_tablespace_oid  									2
#define Anum_gp_persistent_tablespace_node_persistent_state      							3
#define Anum_gp_persistent_tablespace_node_reserved				    						4
#define Anum_gp_persistent_tablespace_node_parent_xid  										5
#define Anum_gp_persistent_tablespace_node_persistent_serial_num    						6
#define Anum_gp_persistent_tablespace_node_previous_free_tid								7

typedef FormData_gp_persistent_tablespace_node *Form_gp_persistent_tablespace_node;


/*
 * gp_persistent_tablespace_node table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_gp_persistent_tablespace_node \
{ GpPersistentTablespaceNodeRelationId, {"filespace_oid"},									26, -1, 4, 2, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentTablespaceNodeRelationId, {"tablespace_oid"}, 								26, -1,	4, 3, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentTablespaceNodeRelationId, {"persistent_state"},								21, -1, 2, 4, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ GpPersistentTablespaceNodeRelationId, {"reserved"}, 										23, -1, 4, 7, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentTablespaceNodeRelationId, {"parent_xid"},										23, -1, 4, 8, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentTablespaceNodeRelationId, {"persistent_serial_num"},							20, -1, 8, 9, 0, -1, -1, true, 'p', 'd', true, false, false, true, 0 }, \
{ GpPersistentTablespaceNodeRelationId, {"previous_free_tid"},								27, -1, 6, 10, 0, -1, -1, false, 'p', 's', true, false, false, true, 0 }, \

/*
 * gp_persistent_tablespace_node table values for FormData_pg_class.
 */
#define Class_gp_persistent_tablespace_node \
  {"gp_persistent_tablespace_node"}, PG_CATALOG_NAMESPACE, GP_PERSISTENT_TABLESPACE_NODE_OID, BOOTSTRAP_SUPERUSERID, 0, \
               GpPersistentTablespaceNodeRelationId, GLOBALTABLESPACE_OID, \
               2, 100, 0, 0, 0, 0, false, true, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_gp_persistent_tablespace_node, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}


/*
 * Defines for gp_persistent_filespace_node table
 */
#define GpPersistentFilespaceNodeRelationName		"gp_persistent_filespace_node"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_persistent_filespace_node
   with (shared=true, oid=false, relid=5093, reltype_oid=6993, content=PERSISTENT)
   (
   filespace_oid                                 oid      ,
   db_id                                         smallint ,
   location                                    text     ,
   persistent_state                              smallint ,
   reserved                                      integer  ,
   parent_xid                                    integer  ,
   persistent_serial_num                         bigint   ,
   previous_free_tid                             tid      ,
   );

   TIDYCAT_ENDFAKEDEF
*/


#define GpPersistentFilespaceNodeRelationId 5093

CATALOG(gp_persistent_filespace_node,5093) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid			filespace_oid;
	int2		db_id;
	text		location;
	int2		persistent_state;
	int4		reserved;
	int4		parent_xid;
	int8		persistent_serial_num;
	tid			previous_free_tid;
} FormData_gp_persistent_filespace_node;

#define Natts_gp_persistent_filespace_node				    	 						8
#define Anum_gp_persistent_filespace_node_filespace_oid  		 						1
#define Anum_gp_persistent_filespace_node_db_id  				 						2
#define Anum_gp_persistent_filespace_node_location			 							3
#define Anum_gp_persistent_filespace_node_persistent_state       						4
#define Anum_gp_persistent_filespace_node_reserved				 						5
#define Anum_gp_persistent_filespace_node_parent_xid  			 						6
#define Anum_gp_persistent_filespace_node_persistent_serial_num  						7
#define Anum_gp_persistent_filespace_node_previous_free_tid		 						8

typedef FormData_gp_persistent_filespace_node *Form_gp_persistent_filespace_node;


/*
 * gp_persistent_filespace_node table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 *
 * NOTE: The use of type text triggers all columns afterward to be NOT NULL....
 */
#define Schema_gp_persistent_filespace_node \
{ GpPersistentFilespaceNodeRelationId, {"filespace_oid"}, 									26, -1,	4, 1, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ GpPersistentFilespaceNodeRelationId, {"db_id"},											21, -1, 2, 3, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ GpPersistentFilespaceNodeRelationId, {"location"},										25, -1, -1, 4, 0, -1, -1, false, 'x', 'i', false, false, false, true, 0 }, \
{ GpPersistentFilespaceNodeRelationId, {"persistent_state"},								21, -1, 2, 7, 0, -1, -1, true, 'p', 's', false, false, false, true, 0 }, \
{ GpPersistentFilespaceNodeRelationId, {"reserved"},										23, -1, 4, 10, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ GpPersistentFilespaceNodeRelationId, {"parent_xid"},										23, -1, 4, 11, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ GpPersistentFilespaceNodeRelationId, {"persistent_serial_num"},							20, -1, 8, 12, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }, \
{ GpPersistentFilespaceNodeRelationId, {"previous_free_tid"},								27, -1, 6, 13, 0, -1, -1, false, 'p', 's', false, false, false, true, 0 }, \

/*
 * gp_persistent_filespace_node table values for FormData_pg_class.
 */
#define Class_gp_persistent_filespace_node \
  {"gp_persistent_filespace_node"}, PG_CATALOG_NAMESPACE, GP_PERSISTENT_FILESPACE_NODE_OID, BOOTSTRAP_SUPERUSERID, 0, \
               GpPersistentFilespaceNodeRelationId, GLOBALTABLESPACE_OID, \
               2, 100, 0, 0, 0, 0, false, true, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_gp_persistent_filespace_node, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}



#undef tid
#undef int8
#undef gpxlogloc


// Cannot change this length with upgrade work to convert the file's tuples...
#define FilespaceLocationBlankPaddedWithNullTermLen MAXPGPATH

inline static bool GpPersistent_IsPersistentRelation(
	Oid		testOid)
{
	/*
	 * Persistent related global tables.
	 */
	if (testOid == GpPersistentRelfileNodeRelationId ||	// 5089
		testOid == GpPersistentRelationNodeRelationId || // 5090
		testOid == GpPersistentDatabaseNodeRelationId ||	// 5091
		testOid == GpPersistentTablespaceNodeRelationId ||	// 5092
		testOid == GpPersistentFilespaceNodeRelationId ||	// 5093
		testOid == GpGlobalSequenceRelationId)				// 5096
		return true;

	return false;
}
inline static bool GpPersistent_SkipXLogInfo(
	Oid		testOid)
{
	/*
	 * NOTE: Only shared (i.e. global) relations can be skipped.
	 */
//	Assert(IsSharedRelation(testOid));

	if (GpPersistent_IsPersistentRelation(testOid))
		return true;
	
	/*
	 * pg_shdepend 
	 */
	if (testOid == SharedDependRelationId)					// 1214
		return true;

	return false;
}

extern void GpPersistentRelfileNode_GetRelfileInfo(
	char		relkind,			/* see RELKIND_xxx constants */
	char		relstorage,			/* see RELSTORAGE_xxx constants */
	Oid			relam,				/* index access method; 0 if not an index */

	PersistentFileSysRelStorageMgr			*relfileStorageManager,
	PersistentFileSysRelBufpoolKind			*relfileBufpoolKind);

extern void GpPersistentRelfileNode_GetValues(
	Datum									*values,
	Oid 									*tablespaceOid,
	Oid 									*databaseOid,
	Oid 									*relfilenodeOid,
	int32									*segmentFileNum,
	PersistentFileSysRelStorageMgr			*relationStorageManager,
	PersistentFileSysState					*persistentState,
	PersistentFileSysRelBufpoolKind 		*relBufpoolKind,
	TransactionId							*parentXid,
	int64									*persistentSerialNum,
	ItemPointerData 						*previousFreeTid,
	bool									*sharedStorage
	);

extern void GpPersistentRelfileNode_SetDatumValues(
	Datum									*values,

	Oid 									tablespaceOid,
	Oid 									databaseOid,
	Oid 									relfilenodeOid,
	int32									segmentFileNum,
	PersistentFileSysRelStorageMgr			relationStorageManager,
	PersistentFileSysState					persistentState,
	PersistentFileSysRelBufpoolKind 		relBufpoolKind,
	TransactionId							parentXid,
	int64									persistentSerialNum,
	ItemPointerData 						*previousFreeTid,
	bool									sharedStorage);

extern void GpPersistentRelationNode_GetValues(
		Datum *values,
		Oid *tablespaceOid,
		Oid *databaseOid,
		Oid *relfilenodeOid,
		PersistentFileSysState *persistentState,
		int32 *reserved,
		TransactionId *parentXid,
		int64 *persistentSerialNum,
		ItemPointerData *previousFreeTid,
		bool *sharedStorage);

extern void GpPersistentRelationNode_SetDatumValues(
									Datum *values,
									Oid tablespaceOid,
									Oid databaseOid,
									Oid relfilenodeOid,
									PersistentFileSysState persistentState,
									int32 reserved,
									TransactionId parentXid,
									int64 persistentSerialNum,
									ItemPointerData *previousFreeTid,
									bool sharedStorage);

extern void GpPersistentDatabaseNode_GetValues(
	Datum							*values,

	Oid 							*tablespaceOid,
	Oid 							*databaseOid,
	PersistentFileSysState			*persistentState,
	int32							*reserved,
	TransactionId					*parentXid,
	int64							*persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							*sharedStorage);

extern void GpPersistentDatabaseNode_SetDatumValues(
	Datum							*values,

	Oid 							tablespaceOid,
	Oid 							databaseOid,
	PersistentFileSysState			persistentState,
	int32							reserved,
	TransactionId					parentXid,
	int64							persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							sharedStorage);

extern void GpPersistentTablespaceNode_GetValues(
	Datum							*values,
	Oid 							*filespaceOid,
	Oid 							*tablespaceOid,
	PersistentFileSysState			*persistentState,
	int32							*reserved,
	TransactionId					*parentXid,
	int64							*persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							*sharedStorage);

extern void GpPersistentTablespaceNode_SetDatumValues(
	Datum							*values,

	Oid 							filespaceOid,
	Oid 							tablespaceOid,
	PersistentFileSysState			persistentState,
	TransactionId					parentXid,
	int64							persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							sharedStorage);

extern void GpPersistentFilespaceNode_GetValues(
	Datum							*values,
	Oid 							*filespaceOid,
	int16							*dbId1,
	char							locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen],
	PersistentFileSysState			*persistentState,
	int32							*reserved,
	TransactionId					*parentXid,
	int64							*persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							*sharedStorage);

extern void GpPersistentFilespaceNode_SetDatumValues(
	Datum							*values,

	Oid 							filespaceOid,
	char							locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen],
	PersistentFileSysState			persistentState,
	int32							reserved,
	TransactionId					parentXid,
	int64							persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							sharedStorage);

extern void GpPersistent_GetCommonValues(
	PersistentFsObjType 			fsObjType,
	Datum							*values,
	PersistentFileSysObjName		*fsObjName,
	PersistentFileSysState			*persistentState,
	TransactionId					*parentXid,
	int64							*persistentSerialNum);

extern void GpRelfileNode_GetValues(
	Datum							*values,

	Oid 							*relfilenodeOid,
	int32							*segmentFileNum,
	ItemPointer						persistentTid,
	int64							*persistentSerialNum);

extern void GpRelfileNode_SetDatumValues(
	Datum							*values,

	Oid 							relfilenodeOid,
	int32							segmentFileNum,
	ItemPointer		 				persistentTid,
	int64							persistentSerialNum);


#endif   /* GP_PERSISTENT_H */

