%{
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
 * bootparse.y
 *	  yacc grammar for the "bootstrap" mode (BKI file format)
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/bootstrap/bootparse.y,v 1.84 2006/08/25 04:06:46 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "access/attnum.h"
#include "access/htup.h"
#include "access/itup.h"
#include "access/skey.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "catalog/catalog.h"
#include "catalog/gp_configuration.h"
#include "catalog/gp_persistent.h"
#include "catalog/gp_global_sequence.h"
#include "catalog/gp_san_config.h"
#include "catalog/gp_segment_config.h"
#include "catalog/gp_verification_history.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly_alter_column.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_filespace_entry.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_tidycat.h"
#include "catalog/toasting.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "rewrite/prs2lock.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "storage/smgr.h"
#include "tcop/dest.h"
#include "utils/rel.h"

#define atooid(x)	((Oid) strtoul((x), NULL, 10))

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

static void
do_start(void)
{
	StartTransactionCommand();
	elog(DEBUG4, "start transaction");
}


static void
do_end(void)
{
	CommitTransactionCommand();
	elog(DEBUG4, "commit transaction");
	CHECK_FOR_INTERRUPTS();		/* allow SIGINT to kill bootstrap run */
	if (isatty(0))
	{
		printf("bootstrap> ");
		fflush(stdout);
	}
}


int num_columns_read = 0;

%}

%expect 0
%name-prefix="boot_yy"

%union
{
	List		*list;
	IndexElem	*ielem;
	char		*str;
	int			ival;
	Oid			oidval;
}

%type <list>  boot_index_params
%type <ielem> boot_index_param
%type <ival>  boot_const boot_ident
%type <ival>  optbootstrap optsharedrelation optwithoutoids
%type <ival>  boot_tuple boot_tuplelist
%type <oidval> oidspec optoideq

%token <ival> CONST_P ID
%token OPEN XCLOSE XCREATE INSERT_TUPLE
%token XDECLARE INDEX ON USING XBUILD INDICES UNIQUE XTOAST
%token COMMA EQUALS LPAREN RPAREN
%token OBJ_ID XBOOTSTRAP XSHARED_RELATION XWITHOUT_OIDS NULLVAL
%start TopLevel

%nonassoc low
%nonassoc high

%%

TopLevel:
		  Boot_Queries
		|
		;

Boot_Queries:
		  Boot_Query
		| Boot_Queries Boot_Query
		;

Boot_Query :
		  Boot_OpenStmt
		| Boot_CloseStmt
		| Boot_CreateStmt
		| Boot_InsertStmt
		| Boot_DeclareIndexStmt
		| Boot_DeclareUniqueIndexStmt
		| Boot_DeclareToastStmt
		| Boot_BuildIndsStmt
		;

Boot_OpenStmt:
		  OPEN boot_ident
				{
					do_start();
					boot_openrel(LexIDStr($2));
					do_end();
				}
		;

Boot_CloseStmt:
		  XCLOSE boot_ident %prec low
				{
					do_start();
					closerel(LexIDStr($2));
					do_end();
				}
		| XCLOSE %prec high
				{
					do_start();
					closerel(NULL);
					do_end();
				}
		;

Boot_CreateStmt:
		  XCREATE optbootstrap optsharedrelation optwithoutoids boot_ident oidspec LPAREN
				{
					do_start();
					numattr = 0;
					elog(DEBUG4, "creating%s%s relation %s %u",
						 $2 ? " bootstrap" : "",
						 $3 ? " shared" : "",
						 LexIDStr($5),
						 $6);
				}
		  boot_typelist
				{
					do_end();
				}
		  RPAREN
				{
					TupleDesc tupdesc;

					do_start();

					tupdesc = CreateTupleDesc(numattr, !($4), attrtypes);

					if ($2)
					{
						if (boot_reldesc)
						{
							elog(DEBUG4, "create bootstrap: warning, open relation exists, closing first");
							closerel(NULL);
						}

						boot_reldesc = heap_create(LexIDStr($5),
												   PG_CATALOG_NAMESPACE,
												   $3 ? GLOBALTABLESPACE_OID : 0,
												   $6,
												   tupdesc,
												   /* relam */ InvalidOid,
												   RELKIND_RELATION,
												   RELSTORAGE_HEAP,
												   $3,
												   true,
												   /* bufferPoolBulkLoad */ false);
						elog(DEBUG4, "bootstrap relation created");
					}
					else
					{
						Oid id;
						Oid typid = InvalidOid;

						/* boot strap new table types for 3.3 */
						switch ($6)
						{
							case AppendOnlyAlterColumnRelationId:
								typid = PG_APPENDONLY_ALTER_COLUMN_OID;
								break;
							case FileSpaceRelationId:
								typid = PG_FILESPACE_OID;
								break;
							case StatLastOpRelationId: 
								/* MPP-6929: metadata tracking */
								typid = PG_STAT_LAST_OPERATION_OID;
								break;
							case StatLastShOpRelationId: 
								/* MPP-6929: metadata tracking */
								typid = PG_STAT_LAST_SHOPERATION_OID;
								break;

							/* Four new tables in 4.0 for foreign data */
							case ForeignDataWrapperRelationId:
								typid = PG_FOREIGN_DATA_WRAPPER_OID;
								break;
							case ForeignServerRelationId:
								typid = PG_FOREIGN_SERVER_OID;
								break;
							case UserMappingRelationId:
								typid = PG_USER_MAPPING_OID;
								break;
							case ForeignTableRelationId:
								typid = PG_FOREIGN_TABLE_OID;
								break;

							/* new tables in 4.0 for persistent file ops */
							case GpPersistentRelfileNodeRelationId:
								typid = GP_PERSISTENT_RELFILE_NODE_OID;
								break;
                            case GpPersistentRelationNodeRelationId:
                                typid = GP_PERSISTENT_RELATION_NODE_OID;
                                break;
							case GpPersistentDatabaseNodeRelationId:
								typid = GP_PERSISTENT_DATABASE_NODE_OID;
								break;
							case GpPersistentTablespaceNodeRelationId:
								typid = GP_PERSISTENT_TABLESPACE_NODE_OID;
								break;
							case GpPersistentFilespaceNodeRelationId:
								typid = GP_PERSISTENT_FILESPACE_NODE_OID;
								break;
							case GpRelfileNodeRelationId:
								typid = GP_RELFILE_NODE_OID;
								break;

/* TIDYCAT_BEGIN_CODEGEN 
*/
/*
   WARNING: DO NOT MODIFY THE FOLLOWING SECTION: 
   Generated by ./tidycat.pl version 31
   on Thu May 24 14:25:58 2012
 */
/* relation id: 5035 - gp_san_configuration 20101104 */
							case GpSanConfigRelationId:
								typid = GP_SAN_CONFIGURATION_RELTYPE_OID;
								break;
/* relation id: 5039 - gp_fault_strategy 20101104 */
							case GpFaultStrategyRelationId:
								typid = GP_FAULT_STRATEGY_RELTYPE_OID;
								break;
/* relation id: 5096 - gp_global_sequence 20101104 */
							case GpGlobalSequenceRelationId:
								typid = GP_GLOBAL_SEQUENCE_RELTYPE_OID;
								break;
/* relation id: 5006 - gp_configuration_history 20101104 */
							case GpConfigHistoryRelationId:
								typid = GP_CONFIGURATION_HISTORY_RELTYPE_OID;
								break;
/* relation id: 5029 - gp_db_interfaces 20101104 */
							case GpDbInterfacesRelationId:
								typid = GP_DB_INTERFACES_RELTYPE_OID;
								break;
/* relation id: 5030 - gp_interfaces 20101104 */
							case GpInterfacesRelationId:
								typid = GP_INTERFACES_RELTYPE_OID;
								break;
/* relation id: 5036 - gp_segment_configuration 20101104 */
							case GpSegmentConfigRelationId:
								typid = GP_SEGMENT_CONFIGURATION_RELTYPE_OID;
								break;
/* relation id: 5033 - pg_filespace_entry 20101122 */
							case FileSpaceEntryRelationId:
								typid = PG_FILESPACE_ENTRY_RELTYPE_OID;
								break;
/* relation id: 7175 - pg_extprotocol 20110526 */
							case ExtprotocolRelationId:
								typid = PG_EXTPROTOCOL_RELTYPE_OID;
								break;
/* relation id: 3231 - pg_attribute_encoding 20110727 */
							case AttributeEncodingRelationId:
								typid = PG_ATTRIBUTE_ENCODING_RELTYPE_OID;
								break;
/* relation id: 3220 - pg_type_encoding 20110727 */
							case TypeEncodingRelationId:
								typid = PG_TYPE_ENCODING_RELTYPE_OID;
								break;
/* relation id: 6429 - gp_verification_history 20110609 */
							case GpVerificationHistoryRelationId:
								typid = GP_VERIFICATION_HISTORY_RELTYPE_OID;
								break;
/* relation id: 3124 - pg_proc_callback 20110829 */
							case ProcCallbackRelationId:
								typid = PG_PROC_CALLBACK_RELTYPE_OID;
								break;
/* relation id: 9903 - pg_partition_encoding 20110814 */
							case PartitionEncodingRelationId:
								typid = PG_PARTITION_ENCODING_RELTYPE_OID;
								break;
/* relation id: 2914 - pg_auth_time_constraint 20110908 */
                            case AuthTimeConstraintRelationId:
                                typid = PG_AUTH_TIME_CONSTRAINT_RELTYPE_OID;
                                break;
/* relation id: 3056 - pg_compression 20110901 */
							case CompressionRelationId:
								typid = PG_COMPRESSION_RELTYPE_OID;
								break;
/* relation id: 6112 - pg_filesystem 20130123 */
							case FileSystemRelationId:
								typid = PG_FILESYSTEM_RELTYPE_OID;
								break;

/* relation id: 7076 - pg_remote_credentials 20140205 */
							case RemoteCredentialsRelationId:
								typid = PG_REMOTE_CREDENTIALS_RELTYPE_OID;
								break;

/* relation id: 6026 - pg_resqueue 20150917 */
							case ResQueueRelationId:
								typid = PG_RESQUEUE_RELTYPE_OID;
								break;

/* TIDYCAT_END_CODEGEN */

							default:
								break;
						}

						id = heap_create_with_catalog(LexIDStr($5),
													  PG_CATALOG_NAMESPACE,
													  $3 ? GLOBALTABLESPACE_OID : 0,
													  $6,
													  BOOTSTRAP_SUPERUSERID,
													  tupdesc,
													  /* relam */ InvalidOid,
													  RELKIND_RELATION,
													  RELSTORAGE_HEAP,
													  $3,
													  true,
													  /* bufferPoolBulkLoad */ false,
													  0,
													  ONCOMMIT_NOOP,
													  NULL,			/*CDB*/
													  (Datum) 0,
													  true,
													  &typid,
						 					  		  /* persistentTid */ NULL,
						 					  		  /* persistentSerialNum */ NULL);
						elog(DEBUG4, "relation created with oid %u", id);
					}
					do_end();
				}
		;

Boot_InsertStmt:
		  INSERT_TUPLE optoideq
				{
					do_start();
					if ($2)
						elog(DEBUG4, "inserting row with oid %u", $2);
					else
						elog(DEBUG4, "inserting row");
					num_columns_read = 0;
				}
		  LPAREN  boot_tuplelist RPAREN
				{
					if (num_columns_read != numattr)
						elog(ERROR, "incorrect number of columns in row (expected %d, got %d)",
							 numattr, num_columns_read);
					if (boot_reldesc == NULL)
						elog(FATAL, "relation not open");
					InsertOneTuple($2);
					do_end();
				}
		;

Boot_DeclareIndexStmt:
		  XDECLARE INDEX boot_ident oidspec ON boot_ident USING boot_ident LPAREN boot_index_params RPAREN
				{
					Oid     relationId;
					do_start();

					relationId = RangeVarGetRelid(makeRangeVar(NULL, NULL, LexIDStr($6), -1), 
								false, false);

					DefineIndex(relationId,
								LexIDStr($3),
								$4,
								LexIDStr($8),
								NULL,
								$10,
								NULL, NIL, NIL,
								false, false, false,
								false, false, true, false, false,
								false, NULL);
					do_end();
				}
		;

Boot_DeclareUniqueIndexStmt:
		  XDECLARE UNIQUE INDEX boot_ident oidspec ON boot_ident USING boot_ident LPAREN boot_index_params RPAREN
				{
					Oid     relationId;
					do_start();

					relationId = RangeVarGetRelid(makeRangeVar(NULL, NULL, LexIDStr($7), -1),
								false, false);
                                                  
					DefineIndex(relationId,
								LexIDStr($4),
								$5,
								LexIDStr($9),
								NULL,
								$11,
								NULL, NIL, NIL,
								true, false, false,
								false, false, true, false, false,
								false,
								NULL);
					do_end();
				}
		;

Boot_DeclareToastStmt:
		  XDECLARE XTOAST oidspec oidspec ON boot_ident
				{
					do_start();

					BootstrapToastTable(LexIDStr($6), $3, $4);
					do_end();
				}
		;

Boot_BuildIndsStmt:
		  XBUILD INDICES
				{
					do_start();
					build_indices();
					do_end();
				}
		;


boot_index_params:
		boot_index_params COMMA boot_index_param	{ $$ = lappend($1, $3); }
		| boot_index_param							{ $$ = list_make1($1); }
		;

boot_index_param:
		boot_ident boot_ident
				{
					IndexElem *n = makeNode(IndexElem);
					n->name = LexIDStr($1);
					n->expr = NULL;
					n->opclass = list_make1(makeString(LexIDStr($2)));
					$$ = n;
				}
		;

optbootstrap:
			XBOOTSTRAP	{ $$ = 1; }
		|				{ $$ = 0; }
		;

optsharedrelation:
			XSHARED_RELATION	{ $$ = 1; }
		|						{ $$ = 0; }
		;

optwithoutoids:
			XWITHOUT_OIDS	{ $$ = 1; }
		|					{ $$ = 0; }
		;

boot_typelist:
		  boot_type_thing
		| boot_typelist COMMA boot_type_thing
		;

boot_type_thing:
		  boot_ident EQUALS boot_ident
				{
				   if (++numattr > MAXATTR)
						elog(FATAL, "too many columns");
				   DefineAttr(LexIDStr($1),LexIDStr($3),numattr-1);
				}
		;

oidspec:
			boot_ident							{ $$ = atooid(LexIDStr($1)); }
		;

optoideq:
			OBJ_ID EQUALS oidspec				{ $$ = $3; }
		|										{ $$ = (Oid) 0; }
		;

boot_tuplelist:
		   boot_tuple
		|  boot_tuplelist boot_tuple
		|  boot_tuplelist COMMA boot_tuple
		;

boot_tuple:
		  boot_ident
			{ InsertOneValue(LexIDStr($1), num_columns_read++); }
		| boot_const
			{ InsertOneValue(LexIDStr($1), num_columns_read++); }
		| NULLVAL
			{ InsertOneNull(num_columns_read++); }
		;

boot_const :
		  CONST_P { $$=yylval.ival; }
		;

boot_ident :
		  ID	{ $$=yylval.ival; }
		;
%%

#include "bootscanner.c"
