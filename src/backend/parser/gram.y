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

/*#define YYDEBUG 1*/
/*-------------------------------------------------------------------------
 *
 * gram.y
 *	  POSTGRES SQL YACC rules/actions
 *
 * Portions Copyright (c) 2006-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/parser/gram.y,v 2.568 2006/11/05 22:42:09 tgl Exp $
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Sept, 1994		POSTQUEL to SQL conversion
 *	  Andrew Yu			Oct, 1994		lispy code conversion
 *
 * NOTES
 *	  CAPITALS are used to represent terminal symbols.
 *	  non-capitals are used to represent non-terminals.
 *	  SQL92-specific syntax is separated from plain SQL/Postgres syntax
 *	  to help isolate the non-extensible portions of the parser.
 *
 *	  In general, nothing in this file should initiate database accesses
 *	  nor depend on changeable state (such as SET variables).  If you do
 *	  database accesses, your code will fail when we have aborted the
 *	  current transaction and are just parsing commands to find the next
 *	  ROLLBACK or COMMIT.  If you make use of SET variables, then you
 *	  will do the wrong thing in multi-query strings like this:
 *			SET SQL_inheritance TO off; SELECT * FROM foo;
 *	  because the entire string is parsed by gram.y before the SET gets
 *	  executed.  Anything that depends on the database or changeable state
 *	  should be handled during parse analysis so that it happens at the
 *	  right time not the wrong time.  The handling of SQL_inheritance is
 *	  a good example.
 *
 * WARNINGS
 *	  If you use a list, make sure the datum is a node so that the printing
 *	  routines work.
 *
 *	  Sometimes we assign constants to makeStrings. Make sure we don't free
 *	  those.
 *
 *-------------------------------------------------------------------------
 */
#undef REPEATABLE
#include "postgres.h"

#include <ctype.h>
#include <limits.h>


#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/gramparse.h"
#include "storage/lmgr.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/numeric.h"
#include "cdb/cdbvars.h" /* CDB *//* gp_enable_partitioned_tables */

#include "resourcemanager/resourcemanager.h"


/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (Rhs)[0]; \
	} while (0)

/*
 * The %name-prefix option below will make bison call base_yylex, but we
 * really want it to call filtered_base_yylex (see parser.c).
 */
#define base_yylex filtered_base_yylex

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

extern List *parsetree;			/* final parse result is delivered here */

static bool QueryIsRule = FALSE;

/*
 * If you need access to certain yacc-generated variables and find that
 * they're static by default, uncomment the next line.  (this is not a
 * problem, yet.)
 */
/*#define __YYSCLASS*/

static Node *makeAddPartitionCreateStmt(Node *n, Node *subSpec);
static Node *makeColumnRef(char *colname, List *indirection, int location);
static Node *makeTypeCast(Node *arg, TypeName *typname, int location);
static Node *makeStringConst(char *str, TypeName *typname, int location);
static Node *makeIntConst(int val, int location);
static Node *makeFloatConst(char *str, int location);
static Node *makeAConst(Value *v, int location);
static A_Const *makeBoolAConst(bool state, int location);
static FuncCall *makeOverlaps(List *largs, List *rargs, int location);
static void check_qualified_name(List *names);
static List *check_func_name(List *names);
static List *extractArgTypes(List *parameters);
static SelectStmt *findLeftmostSelect(SelectStmt *node);
static void insertSelectOptions(SelectStmt *stmt,
								List *sortClause, List *lockingClause,
								Node *limitOffset, Node *limitCount,
								WithClause *withClause);
static Node *makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg);
static Node *doNegate(Node *n, int location);
static void doNegateFloat(Value *v);
static List *mergeTableFuncParameters(List *func_args, List *columns);
static TypeName *TableFuncTypeName(List *columns);
static void setWindowExclude(WindowFrame *wframe, WindowExclusion exclude);
static Node *makeIsNotDistinctFromNode(Node *expr, int position);

%}

%expect 0
%name-prefix="base_yy"
%locations

%union
{
	int					ival;
	char				chr;
	char				*str;
	const char			*keyword;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;

	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	FuncWithArgs		*funwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	A_Indices			*aind;
	ResTarget			*target;
	PrivTarget			*privtarget;

	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
	WithClause			*with;
}

%type <node>	stmt schema_stmt
		AlterDatabaseStmt AlterDatabaseSetStmt AlterDomainStmt AlterFdwStmt
		AlterForeignServerStmt AlterGroupStmt
		AlterObjectSchemaStmt AlterOwnerStmt AlterQueueStmt AlterSeqStmt 
		AlterTableStmt AlterUserStmt AlterUserMappingStmt AlterUserSetStmt AlterRoleStmt 
		AlterRoleSetStmt AnalyzeStmt ClosePortalStmt ClusterStmt 
		CommentStmt ConstraintsSetStmt CopyStmt CreateAsStmt CreateCastStmt
		CreateDomainStmt CreateExternalStmt CreateFileSpaceStmt CreateGroupStmt
		CreateOpClassStmt CreatePLangStmt
		CreateQueueStmt CreateSchemaStmt CreateSeqStmt CreateStmt 
		CreateTableSpaceStmt CreateFdwStmt CreateForeignServerStmt CreateForeignStmt 
		CreateAssertStmt CreateTrigStmt 
		CreateUserStmt CreateUserMappingStmt CreateRoleStmt
		CreatedbStmt DeclareCursorStmt DefineStmt DeleteStmt
		DropGroupStmt DropOpClassStmt DropPLangStmt DropQueueStmt DropStmt
		DropAssertStmt DropTrigStmt DropRuleStmt DropCastStmt DropRoleStmt
		DropUserStmt DropdbStmt DropFdwStmt
		DropForeignServerStmt DropUserMappingStmt ExplainStmt 
		ExtTypedesc FetchStmt
		GrantStmt GrantRoleStmt IndexStmt InsertStmt ListenStmt LoadStmt
		LockStmt NotifyStmt OptSingleRowErrorHandling ExplainableStmt PreparableStmt
		CreateFunctionStmt AlterFunctionStmt ReindexStmt RemoveAggrStmt
		RemoveFuncStmt RemoveOperStmt RenameStmt RevokeStmt RevokeRoleStmt
		RuleActionStmt RuleActionStmtOrEmpty RuleStmt
		SelectStmt TransactionStmt TruncateStmt
		UnlistenStmt UpdateStmt VacuumStmt
		VariableResetStmt VariableSetStmt VariableShowStmt
		ViewStmt CheckPointStmt CreateConversionStmt
		DeallocateStmt PrepareStmt ExecuteStmt
		DropOwnedStmt ReassignOwnedStmt
		AlterTypeStmt

%type <node>    deny_login_role deny_interval deny_point deny_day_specifier

%type <node>	select_no_parens select_with_parens select_clause
				simple_select values_clause

%type <node>	alter_column_default opclass_item alter_using
%type <ival>	add_drop

%type <node>	alter_table_cmd alter_rel_cmd alter_table_partition_id_spec
				alter_table_partition_cmd
				alter_table_partition_id_spec_with_opt_default
%type <list>	alter_table_cmds alter_rel_cmds 
				part_values_clause multi_spec_value_list part_values_single
%type <ival>	opt_table_partition_exchange_validate partition_hash_keyword
				partition_coalesce_keyword

%type <dbehavior>	opt_drop_behavior

%type <list>	createdb_opt_list alterdb_opt_list copy_opt_list
				ext_on_clause_list format_opt format_opt_list format_def_list transaction_mode_list
				ext_opt_encoding_list
%type <defelt>	createdb_opt_item alterdb_opt_item copy_opt_item
				ext_on_clause_item format_opt_item format_def_item transaction_mode_item
				ext_opt_encoding_item

%type <ival>	opt_lock lock_type cast_context
%type <boolean>	opt_force opt_or_replace
				opt_grant_grant_option opt_grant_admin_option
				opt_nowait opt_if_exists opt_with_data

%type <list>	OptRoleList
%type <defelt>	OptRoleElem

%type <list> 	OptAlterRoleList
%type <defelt>	OptAlterRoleElem

%type <str>		opt_type
%type <str>		foreign_server_version opt_foreign_server_version
%type <str>		auth_ident

%type <str>		OptSchemaName
%type <list>	OptSchemaEltList

%type <boolean> TriggerActionTime TriggerForSpec opt_trusted
%type <str>		opt_lancompiler

%type <str>		TriggerEvents
%type <value>	TriggerFuncArg

%type <str>		relation_name copy_file_name
				database_name access_method_clause access_method attr_name
				index_name name function_name file_name

%type <list>	func_name handler_name qual_Op qual_all_Op subquery_Op
				opt_class opt_validator validator_clause

%type <range>	qualified_name OptConstrFromTable

%type <str>		all_Op MathOp SpecialRuleRelation

%type <str>		iso_level opt_encoding
%type <node>	grantee
%type <list>	grantee_list
%type <str>		privilege
%type <list>	privileges privilege_list
%type <privtarget> privilege_target
%type <funwithargs> function_with_argtypes
%type <list>	function_with_argtypes_list
%type <chr> 	TriggerOneEvent

%type <list>	stmtblock stmtmulti
				OptTableElementList OptExtTableElementList TableElementList ExtTableElementList
				OptInherit definition 
				OptWith opt_distinct opt_definition func_args func_args_list
				func_as createfunc_opt_list alterfunc_opt_list
				aggr_args aggr_args_list old_aggr_definition old_aggr_list
				oper_argtypes RuleActionList RuleActionMulti
				cdb_string_list
				opt_column_list columnList opt_name_list exttab_auth_list keyvalue_list
				opt_inherited_column_list
				sort_clause opt_sort_clause sortby_list index_params
				name_list from_clause from_list opt_array_bounds
				qualified_name_list any_name any_name_list
				any_operator expr_list attrs
				target_list insert_column_list set_target_list
				set_clause_list set_clause multiple_set_clause
				ctext_expr_list ctext_row def_list indirection opt_indirection
				group_clause group_elem_list group_elem TriggerFuncArgs select_limit
				opt_select_limit opclass_item_list
				transaction_mode_list_or_empty
				TableFuncElementList
				prep_type_clause prep_type_list
				create_generic_options alter_generic_options
				execute_param_clause using_clause returning_clause
				table_func_column_list scatter_clause
%type <node>    table_value_select_clause

%type <range>	OptTempTableName OptErrorTableName
%type <into>	into_clause create_as_target

%type <defelt>	createfunc_opt_item common_func_opt_item
%type <fun_param> func_arg table_func_column
%type <fun_param_mode> arg_class
%type <typnam>	func_return func_type

%type <boolean>  TriggerForType OptTemp OptWeb OptWritable OptSrehLimitType OptSrehKeep
%type <oncommit> OnCommitOption

%type <node>	for_locking_item
%type <list>	for_locking_clause opt_for_locking_clause for_locking_items
%type <list>	locked_rels_list
%type <boolean>	opt_all

%type <node>	join_outer join_qual
%type <jtype>	join_type

%type <list>	extract_list overlay_list position_list
%type <list>	substr_list trim_list
%type <ival>	opt_interval
%type <node>	overlay_placing substr_from substr_for

%type <boolean> opt_instead opt_analyze
%type <boolean> index_opt_unique opt_verbose opt_full
%type <boolean> opt_freeze opt_default opt_ordered opt_recheck
%type <boolean> opt_rootonly_all
%type <boolean> opt_dxl
%type <defelt>	opt_binary opt_oids copy_delimiter

%type <boolean> copy_from opt_hold

%type <ival>	opt_column event cursor_options
%type <objtype>	reindex_type drop_type comment_type

%type <node>	fetch_direction select_limit_value select_offset_value
				select_offset_value2 opt_select_fetch_first_value
%type <ival>	row_or_rows first_or_next

%type <list>	OptSeqList
%type <defelt>	OptSeqElem

%type <istmt>	insert_rest

%type <vsetstmt> set_rest
%type <node>	TableElement ExtTableElement ConstraintElem TableFuncElement
%type <node>	columnDef ExtcolumnDef
%type <node>	cdb_string
%type <defelt>	def_elem old_aggr_elem keyvalue_pair
%type <node>	def_arg columnElem where_clause where_or_current_clause
				a_expr b_expr c_expr simple_func func_expr AexprConst indirection_el
				columnref in_expr having_clause func_table array_expr
%type <list>	window_definition_list window_clause
%type <boolean>	window_frame_units
%type <ival>	window_frame_exclusion
%type <node>	window_spec
%type <node>	window_frame_extent
				window_frame_start window_frame_preceding window_frame_between
				window_frame_bound window_frame_following 
				window_frame_clause opt_window_frame_clause
%type <list>	window_partition_clause opt_window_partition_clause
				opt_window_order_clause
%type <str>		opt_window_name window_name

%type <list>	row type_list array_expr_list
%type <node>	case_expr case_arg when_clause when_operand case_default
%type <list>	when_clause_list
%type <node>	decode_expr search_result decode_default
%type <list>	search_result_list
%type <ival>	sub_type
%type <list>	OptCreateAs CreateAsList
%type <node>	CreateAsElement ctext_expr
%type <value>	NumericOnly FloatOnly IntegerOnly
%type <alias>	alias_clause
%type <sortby>	sortby
%type <ielem>	index_elem
%type <node>	table_ref
%type <jexpr>	joined_table
%type <range>	relation_expr
%type <range>	relation_expr_opt_alias
%type <target>	target_el single_set_clause set_target insert_column_item

%type <str>		generic_option_name
%type <node>	generic_option_arg
%type <defelt>  generic_option_elem alter_generic_option_elem
%type <list>	generic_option_list alter_generic_option_list

%type <typnam>	Typename SimpleTypename ConstTypename
				GenericType Numeric opt_float
				Character ConstCharacter
				CharacterWithLength CharacterWithoutLength
				ConstDatetime ConstInterval
				Bit ConstBit BitWithLength BitWithoutLength
%type <str>		character
%type <str>		extract_arg
%type <str>		opt_charset
%type <ival>	opt_numeric opt_decimal
%type <boolean> opt_varying opt_timezone

%type <ival>	Iconst SignedIconst
%type <str>		Sconst comment_text
%type <str>		RoleId opt_granted_by opt_boolean ColId_or_Sconst
%type <str>		QueueId
%type <list>	var_list var_list_or_default
%type <str>		ColId ColLabel ColLabelNoAs var_name type_name param_name
%type <keyword> PartitionIdentKeyword	
%type <str>		PartitionColId
%type <node>	var_value zone_value

%type <keyword> unreserved_keyword func_name_keyword
%type <keyword> col_name_keyword reserved_keyword
%type <keyword> keywords_ok_in_alias_no_as

%type <node>	TableConstraint TableLikeClause 
%type <list>	TableLikeOptionList
%type <ival>	TableLikeOption
%type <list>	ColQualList
%type <node>	ColConstraint ColConstraintElem ConstraintAttr
%type <ival>	key_actions key_delete key_match key_update key_action
%type <ival>	ConstraintAttributeSpec ConstraintDeferrabilitySpec
				ConstraintTimeSpec

%type <list>	constraints_set_list
%type <boolean> constraints_set_mode
%type <str>		OptTableSpace OptConsTableSpace OptOwner
%type <str>		OptStorage
%type <list>    DistributedBy OptDistributedBy 
%type <ival>	TabPartitionByType OptTabPartitionRangeInclusive
%type <node>	OptTabPartitionBy TabSubPartitionBy 
				tab_part_val tab_part_val_no_paran
%type <node>	list_subparts opt_list_subparts
%type <list>	opt_check_option
%type <node>	OptTabPartitionsNumber OptTabSubPartitionsNumber 
%type <node>	OptTabPartitionSpec OptTabSubPartitionSpec TabSubPartitionTemplate      /* PartitionSpec */
%type <list>	TabPartitionElemList TabSubPartitionElemList /* list of PartitionElem */

%type <node> 	TabPartitionElem TabSubPartitionElem  /* PartitionElem */

%type <node> 	TabPartitionBoundarySpec OptTabPartitionBoundarySpec  /* PartitionBoundSpec */
%type <list> 	TabPartitionBoundarySpecValList
				OptTabPartitionBoundarySpecValList
				part_values_or_spec_list
%type <node> 	TabPartitionBoundarySpecStart TabPartitionBoundarySpecEnd
				OptTabPartitionBoundarySpecEnd        /* PartitionRangeItem */
%type <node> 	OptTabPartitionBoundarySpecEvery      /* PartitionRangeItem */
%type <node> 	TabPartitionNameDecl TabSubPartitionNameDecl      /* string */
				TabPartitionDefaultNameDecl TabSubPartitionDefaultNameDecl 
				opt_table_partition_merge_into 
				table_partition_modify
				opt_table_partition_split_into
%type <boolean>	opt_comma
%type <node> 	OptTabPartitionStorageAttr

%type <node> 	common_table_expr
%type <with> 	with_clause
%type <list>	cte_list
%type <node>	opt_time

%type <node>	column_reference_storage_directive
%type <list>	opt_storage_encoding OptTabPartitionColumnEncList
				TabPartitionColumnEncList
%type <value>	size_unit 
%type <list>	resqueue_attr_definition resqueue_attr_def_list 
%type <defelt>  resqueue_attr_def_elem

/*
 * If you make any token changes, update the keyword table in
 * parser/keywords.c and add new keywords to the appropriate one of
 * the reserved-or-not-so-reserved keyword lists, below; search
 * this file for "Name classification hierarchy".
 */

/* ordinary key words in alphabetical order */
%token <keyword> ABORT_P ABSOLUTE_P ACCESS ACTION ACTIVE ADD_P ADMIN AFTER
	AGGREGATE ALL ALSO ALTER ANALYSE ANALYZE AND ANY ARRAY AS ASC
	ASSERTION ASSIGNMENT ASYMMETRIC AT AUTHORIZATION

	BACKWARD BEFORE BEGIN_P BETWEEN BIGINT BINARY BIT
	BOOLEAN_P BOTH BY

	CACHE CALLED CASCADE CASCADED CASE CAST CHAIN CHAR_P
	CHARACTER CHARACTERISTICS CHECK CHECKPOINT CLASS CLOSE
	CLUSTER COALESCE COLLATE COLUMN COMMENT COMMIT
	COMMITTED CONCURRENTLY CONNECTION CONSTRAINT CONSTRAINTS CONTAINS CONTENT_P CONTINUE_P CONVERSION_P CONVERT COPY COST
	CREATE CREATEDB CREATEEXTTABLE
	CREATEROLE CREATEUSER CROSS CSV CUBE CURRENT CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA
	CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR CYCLE

	DATA_P DATABASE DAY_P DEALLOCATE DEC DECIMAL_P DECLARE DECODE DEFAULT DEFAULTS
	DEFERRABLE DEFERRED DEFINER DELETE_P DELIMITER DELIMITERS DENY
	DESC DISABLE_P DISTINCT DISTRIBUTED DO DOMAIN_P DOUBLE_P DROP DXL

	EACH ELSE ENABLE_P ENCODING ENCRYPTED END_P ENUM_P ERRORS ESCAPE EVERY EXCEPT 
	EXCHANGE EXCLUDE EXCLUDING EXCLUSIVE EXECUTE EXISTS EXPLAIN EXTERNAL EXTRACT

	FALSE_P FETCH FIELDS FILESPACE FILESYSTEM FILL FILTER FIRST_P FLOAT_P FOLLOWING FOR 
    	FORCE FOREIGN FORMAT FORMATTER FORWARD FREEZE FROM FULL FUNCTION

	GB GLOBAL GRANT GRANTED GREATEST GROUP_P GROUP_ID GROUPING

	HANDLER HASH HAVING HEADER_P HOLD HOST HOUR_P

	IDENTITY_P IF_P  IGNORE_P ILIKE IMMEDIATE IMMUTABLE IMPLICIT_P IN_P INCLUDING
	INCLUSIVE
	INCREMENT
	INDEX INDEXES INHERIT INHERITS INITIALLY INNER_P INOUT INPUT_P
	INSENSITIVE INSERT INSTEAD INT_P INTEGER INTERSECT
	INTERVAL INTO INVOKER IS ISNULL ISOLATION

	JOIN

	KB KEEP KEY

	LANCOMPILER LANGUAGE LARGE_P  LAST_P LEADING LEAST LEFT LEVEL
	LIKE LIMIT LIST LISTEN LOAD LOCAL LOCALTIME LOCALTIMESTAMP LOCATION
	LOCK_P LOG_P LOGIN_P

	MAPPING MASTER MATCH MAXVALUE MB MEDIAN MERGE MINUTE_P MINVALUE MIRROR
	MISSING MODE MODIFIES MODIFY MONTH_P MOVE

	NAME_P NAMES NATIONAL NATURAL NCHAR NEW NEWLINE NEXT NO NOCREATEDB NOCREATEEXTTABLE
	NOCREATEROLE NOCREATEUSER NOINHERIT NOLOGIN_P NONE NOOVERCOMMIT NOSUPERUSER
	NOT NOTHING NOTIFY NOTNULL NOWAIT NULL_P NULLS_P NULLIF NUMERIC

	OBJECT_P OF OFF OFFSET OIDS OLD ON ONLY OPERATOR OPTION OPTIONS OR
	ORDER ORDERED OTHERS OUT_P OUTER_P OVER OVERCOMMIT OVERLAPS OVERLAY OWNED OWNER

	PARTIAL PARTITION PARTITIONS PASSWORD PB PERCENT PERCENTILE_CONT PERCENTILE_DISC
	PLACING POSITION PRECEDING PRECISION PRESERVE PREPARE PREPARED PRIMARY
	PRIOR PRIVILEGES PROCEDURAL PROCEDURE PROTOCOL

	QUEUE QUOTE

	RANDOMLY RANGE READ READABLE READS REAL REASSIGN RECHECK RECURSIVE 
    REFERENCES REINDEX REJECT_P RELATIVE_P 
	RELEASE RENAME REPEATABLE REPLACE RESET RESOURCE RESTART RESTRICT 
	RETURNING RETURNS REVOKE RIGHT
	ROLE ROLLBACK ROLLUP ROOTPARTITION ROW ROWS RULE

	SAVEPOINT SCATTER SCHEMA SCROLL SEARCH SECOND_P 
    SECURITY SEGMENT SELECT SEQUENCE
	SERIALIZABLE SERVER SESSION SESSION_USER SET SETOF SETS SHARE
	SHOW SIMILAR SIMPLE SMALLINT SOME SPLIT SQL STABLE START STATEMENT
	STATISTICS STDIN STDOUT STORAGE STRICT_P 
	SUBPARTITION SUBPARTITIONS
	SUBSTRING SUPERUSER_P SYMMETRIC
	SYSID SYSTEM_P

	TABLE TABLESPACE TB TEMP TEMPLATE TEMPORARY THEN THRESHOLD TIES TIME TIMESTAMP
	TO TRAILING TRANSACTION TREAT TRIGGER TRIM TRUE_P
	TRUNCATE TRUSTED TYPE_P

	UNBOUNDED UNCOMMITTED UNENCRYPTED UNION UNIQUE UNKNOWN UNLISTEN UNTIL
	UPDATE USER USING

	VACUUM VALID VALIDATION VALIDATOR VALUE_P VALUES VARCHAR VARYING
	VERBOSE VERSION_P VIEW VOLATILE

	WEB WHEN WHERE WINDOW WITH WITHIN WITHOUT WORK WRAPPER WRITABLE WRITE

	YEAR_P

	ZONE

/* The grammar thinks these are keywords, but they are not in the kwlist.h
 * list and so can never be entered directly.  The filter in parser.c
 * creates these tokens when required.
 */
%token			NULLS_FIRST NULLS_LAST WITH_CASCADED WITH_LOCAL WITH_CHECK WITH_TIME

/* Special token types, not actually keywords - see the "lex" file */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op
%token <ival>	ICONST PARAM

/* precedence: lowest to highest */
%nonassoc	SET				/* see relation_expr_opt_alias */
%left		UNION EXCEPT
%left		INTERSECT
%left		OR
%left		AND
%right		NOT
%right		'='
%nonassoc	'<' '>'
%nonassoc	LIKE ILIKE SIMILAR
%nonassoc	ESCAPE
%nonassoc	OVERLAPS
%nonassoc	BETWEEN
%nonassoc	IN_P
%left		POSTFIXOP		/* dummy for postfix Op rules */
/*
 * To support target_el without AS, we must give IDENT an explicit priority
 * between POSTFIXOP and Op.  We can safely assign the same priority to
 * various unreserved keywords as needed to resolve ambiguities (this can't
 * have any bad effects since obviously the keywords will still behave the
 * same as if they weren't keywords).  We need to do this for PARTITION,
 * RANGE, ROWS to support opt_existing_window_name; and for RANGE, ROWS
 * so that they can follow a_expr without creating
 * postfix-operator problems.
 */
%nonassoc	IDENT PARTITION RANGE ROWS
/*
 * This is a bit ugly... To allow these to be column aliases without
 * the "AS" keyword, and not conflict with PostgreSQL's non-standard
 * suffix operators, we need to give these a precidence.
 */

%nonassoc   ABORT_P
			%nonassoc ABSOLUTE_P
			%nonassoc ACCESS
			%nonassoc ACTION
			%nonassoc ACTIVE
			%nonassoc ADD_P
			%nonassoc ADMIN
			%nonassoc AFTER
			%nonassoc AGGREGATE
			%nonassoc ALSO
			%nonassoc ALTER
			%nonassoc ASSERTION
			%nonassoc ASSIGNMENT
			%nonassoc BACKWARD
			%nonassoc BEFORE
			%nonassoc BEGIN_P
			%nonassoc BY
			%nonassoc CACHE
			%nonassoc CALLED
			%nonassoc CASCADE
			%nonassoc CASCADED
			%nonassoc CHAIN
			%nonassoc CHARACTERISTICS
			%nonassoc CHECKPOINT
			%nonassoc CLASS
			%nonassoc CLOSE
			%nonassoc CLUSTER
			%nonassoc COMMENT
			%nonassoc COMMIT
			%nonassoc COMMITTED
			%nonassoc CONCURRENTLY
			%nonassoc CONNECTION
			%nonassoc CONSTRAINTS
			%nonassoc CONTAINS
			%nonassoc CONTENT_P
			%nonassoc CONTINUE_P
			%nonassoc CONVERSION_P
			%nonassoc COPY
			%nonassoc COST
			%nonassoc CREATEDB
			%nonassoc CREATEEXTTABLE
			%nonassoc CREATEROLE
			%nonassoc CREATEUSER
			%nonassoc CSV
			%nonassoc CURRENT
			%nonassoc CURSOR
			%nonassoc CYCLE
			%nonassoc DATA_P
			%nonassoc DATABASE
			%nonassoc DAY_P
			%nonassoc DEALLOCATE
			%nonassoc DECLARE
			%nonassoc DEFAULTS
			%nonassoc DEFERRED
			%nonassoc DEFINER
			%nonassoc DELETE_P
			%nonassoc DELIMITER
			%nonassoc DELIMITERS
			%nonassoc DISABLE_P
			%nonassoc DOMAIN_P
			%nonassoc DOUBLE_P
			%nonassoc DROP
			%nonassoc EACH
			%nonassoc ENABLE_P
			%nonassoc ENCODING
			%nonassoc ENCRYPTED
			%nonassoc END_P
			%nonassoc ENUM_P
			%nonassoc ERRORS
			%nonassoc EVERY
			%nonassoc EXCHANGE
			%nonassoc EXCLUDING
			%nonassoc EXCLUSIVE
			%nonassoc EXECUTE
			%nonassoc EXPLAIN
			%nonassoc EXTERNAL
			%nonassoc FETCH
			%nonassoc FIELDS
			%nonassoc FILL
			%nonassoc FIRST_P
			%nonassoc FORCE
			%nonassoc FORMAT
			%nonassoc FORMATTER
			%nonassoc FORWARD
			%nonassoc FUNCTION
			%nonassoc GB
			%nonassoc GLOBAL
			%nonassoc GRANTED
			%nonassoc HANDLER
			%nonassoc HASH
			%nonassoc HEADER_P
			%nonassoc HOLD
			%nonassoc HOST
			%nonassoc HOUR_P
			%nonassoc IF_P
			%nonassoc IMMEDIATE
			%nonassoc IMMUTABLE
			%nonassoc IMPLICIT_P
			%nonassoc INCLUDING
			%nonassoc INCLUSIVE
			%nonassoc INCREMENT
			%nonassoc INDEX
			%nonassoc INDEXES
			%nonassoc INHERIT
			%nonassoc INHERITS
			%nonassoc INPUT_P
			%nonassoc INSENSITIVE
			%nonassoc INSERT
			%nonassoc INSTEAD
			%nonassoc INVOKER
			%nonassoc ISOLATION
			%nonassoc KB
			%nonassoc KEEP
			%nonassoc KEY
			%nonassoc LANCOMPILER
			%nonassoc LANGUAGE
			%nonassoc LARGE_P
			%nonassoc LAST_P
			%nonassoc LEVEL
			%nonassoc LIST
			%nonassoc LISTEN
			%nonassoc LOAD
			%nonassoc LOCAL
			%nonassoc LOCATION
			%nonassoc LOCK_P
			%nonassoc LOGIN_P
			%nonassoc MASTER
			%nonassoc MAPPING
			%nonassoc MATCH
			%nonassoc MAXVALUE
			%nonassoc MB
			%nonassoc MERGE
			%nonassoc MINUTE_P
			%nonassoc MINVALUE
			%nonassoc MIRROR
			%nonassoc MISSING
			%nonassoc MODE
			%nonassoc MODIFIES
			%nonassoc MODIFY
			%nonassoc MONTH_P
			%nonassoc MOVE
			%nonassoc NAME_P
			%nonassoc NAMES
			%nonassoc NEWLINE
			%nonassoc NEXT
			%nonassoc NO
			%nonassoc NOCREATEDB
			%nonassoc NOCREATEEXTTABLE
			%nonassoc NOCREATEROLE
			%nonassoc NOCREATEUSER
			%nonassoc NOINHERIT
			%nonassoc NOLOGIN_P
			%nonassoc NOOVERCOMMIT
			%nonassoc NOSUPERUSER
			%nonassoc NOTHING
			%nonassoc NOTIFY
			%nonassoc NOWAIT
			%nonassoc NULLS_P
			%nonassoc OBJECT_P
			%nonassoc OF
			%nonassoc OIDS
			%nonassoc OPTION
			%nonassoc OPTIONS
			%nonassoc OTHERS
			%nonassoc OVER
			%nonassoc OVERCOMMIT
			%nonassoc OWNED
			%nonassoc OWNER
			%nonassoc PARTIAL
			%nonassoc PARTITIONS
			%nonassoc PASSWORD
			%nonassoc PB
			%nonassoc PERCENT
			%nonassoc PREPARE
			%nonassoc PREPARED
			%nonassoc PRESERVE
			%nonassoc PRIOR
			%nonassoc PRIVILEGES
			%nonassoc PROCEDURAL
			%nonassoc PROCEDURE
			%nonassoc FILESYSTEM
			%nonassoc PROTOCOL
			%nonassoc QUEUE
			%nonassoc QUOTE
			%nonassoc RANDOMLY
			%nonassoc READ
			%nonassoc READABLE
			%nonassoc READS
			%nonassoc REASSIGN
			%nonassoc RECHECK
			%nonassoc RECURSIVE
			%nonassoc REINDEX
			%nonassoc REJECT_P
			%nonassoc RELATIVE_P
			%nonassoc RELEASE
			%nonassoc RENAME
			%nonassoc REPEATABLE
			%nonassoc REPLACE
			%nonassoc RESET
			%nonassoc RESOURCE
			%nonassoc RESTART
			%nonassoc RESTRICT
			%nonassoc RETURNS
			%nonassoc REVOKE
			%nonassoc ROLE
			%nonassoc ROLLBACK
			%nonassoc RULE
			%nonassoc SAVEPOINT
			%nonassoc SCHEMA
			%nonassoc SCROLL
			%nonassoc SEARCH
			%nonassoc SECOND_P
			%nonassoc SECURITY
			%nonassoc SEGMENT
			%nonassoc SEQUENCE
			%nonassoc SERIALIZABLE
			%nonassoc SERVER
			%nonassoc SESSION
			%nonassoc SHARE
			%nonassoc SHOW
			%nonassoc SIMPLE
			%nonassoc SPLIT
			%nonassoc SQL
			%nonassoc STABLE
			%nonassoc START
			%nonassoc STATEMENT
			%nonassoc STATISTICS
			%nonassoc STDIN
			%nonassoc STDOUT
			%nonassoc STORAGE
			%nonassoc SUBPARTITION
			%nonassoc SUBPARTITIONS
			%nonassoc SUPERUSER_P
			%nonassoc SYSID
			%nonassoc SYSTEM_P
			%nonassoc STRICT_P
			%nonassoc TABLESPACE
			%nonassoc TB
			%nonassoc TEMP
			%nonassoc TEMPLATE
			%nonassoc TEMPORARY
			%nonassoc THRESHOLD
			%nonassoc TIES
			%nonassoc TRANSACTION
			%nonassoc TRIGGER
			%nonassoc TRUNCATE
			%nonassoc TRUSTED
			%nonassoc TYPE_P
			%nonassoc UNCOMMITTED
			%nonassoc UNENCRYPTED
			%nonassoc UNLISTEN
			%nonassoc UNTIL
			%nonassoc UPDATE
			%nonassoc VACUUM
			%nonassoc VALID
			%nonassoc VALIDATION
			%nonassoc VALIDATOR
			%nonassoc VALUE_P
			%nonassoc VARYING
			%nonassoc VERSION_P
			%nonassoc VIEW
			%nonassoc VOLATILE
			%nonassoc WEB
			%nonassoc WITH
			%nonassoc WITHIN
			%nonassoc WITHOUT
			%nonassoc WORK
			%nonassoc WRAPPER
			%nonassoc WRITABLE
			%nonassoc WRITE
			%nonassoc YEAR_P
			%nonassoc BIGINT
			%nonassoc BIT
			%nonassoc BOOLEAN_P
			%nonassoc CHAR_P
			%nonassoc CHARACTER
			%nonassoc COALESCE
			%nonassoc CONVERT
			%nonassoc CUBE
			%nonassoc DEC
			%nonassoc DECIMAL_P
			%nonassoc EXISTS
			%nonassoc EXTRACT
			%nonassoc FLOAT_P
			%nonassoc GREATEST
			%nonassoc GROUP_ID
			%nonassoc GROUPING
			%nonassoc INOUT
			%nonassoc INT_P
			%nonassoc INTEGER
			%nonassoc INTERVAL
			%nonassoc LEAST
			%nonassoc MEDIAN
			%nonassoc NATIONAL
			%nonassoc NCHAR
			%nonassoc NONE
			%nonassoc NULLIF
			%nonassoc NUMERIC
			%nonassoc OUT_P
			%nonassoc OVERLAY
			%nonassoc PERCENTILE_CONT
			%nonassoc PERCENTILE_DISC
			%nonassoc POSITION
			%nonassoc PRECISION
			%nonassoc REAL
			%nonassoc ROLLUP
			%nonassoc ROW
			%nonassoc SETOF
			%nonassoc SETS
			%nonassoc SMALLINT
			%nonassoc SUBSTRING
			%nonassoc TIME
			%nonassoc TIMESTAMP
			%nonassoc TREAT
			%nonassoc TRIM
			%nonassoc VALUES
			%nonassoc VARCHAR
			%nonassoc AUTHORIZATION
			%nonassoc BINARY
			%nonassoc FREEZE
			%nonassoc LOG_P
			%nonassoc OUTER_P
			%nonassoc VERBOSE
			


%left		Op OPERATOR		/* multi-character ops and user-defined operators */
%nonassoc	NOTNULL
%nonassoc	ISNULL
%nonassoc	IS NULL_P TRUE_P FALSE_P UNKNOWN /* sets precedence for IS NULL, etc */
%left		'+' '-'
%left		'*' '/' '%'
%left		'^'
/* Unary Operators */
%left		AT ZONE			/* sets precedence for AT TIME ZONE */
%right		UMINUS
%left		'[' ']'
%left		'(' ')'
%left		TYPECAST
%left		'.'
/*
 * These might seem to be low-precedence, but actually they are not part
 * of the arithmetic hierarchy at all in their use as JOIN operators.
 * We make them high-precedence to support their use as function names.
 * They wouldn't be given a precedence at all, were it not that we need
 * left-associativity among the JOIN rules themselves.
 */
%left		JOIN CROSS LEFT FULL RIGHT INNER_P NATURAL
%%

/*
 *	Handle comment-only lines, and ;; SELECT * FROM pg_class ;;;
 *	psql already handles such cases, but other interfaces don't.
 *	bjm 1999/10/05
 */
stmtblock:	stmtmulti								{ parsetree = $1; }
		;

/* the thrashing around here is to discard "empty" statements... */
stmtmulti:	stmtmulti ';' stmt
				{ if ($3 != NULL)
					$$ = lappend($1, $3);
				  else
					$$ = $1;
				}
			| stmt
					{ if ($1 != NULL)
						$$ = list_make1($1);
					  else
						$$ = NIL;
					}
		;

stmt :
			AlterDatabaseStmt
			| AlterDatabaseSetStmt
			| AlterDomainStmt
			| AlterFdwStmt
			| AlterForeignServerStmt
			| AlterFunctionStmt
			| AlterGroupStmt
			| AlterObjectSchemaStmt
			| AlterOwnerStmt
			| AlterQueueStmt
			| AlterRoleSetStmt
			| AlterRoleStmt
			| AlterSeqStmt
			| AlterTableStmt
			| AlterTypeStmt
			| AlterUserMappingStmt
			| AlterUserSetStmt
			| AlterUserStmt
			| AnalyzeStmt
			| CheckPointStmt
			| ClosePortalStmt
			| ClusterStmt
			| CommentStmt
			| ConstraintsSetStmt
			| CopyStmt
			| CreateAsStmt
			| CreateAssertStmt
			| CreateCastStmt
			| CreateConversionStmt
			| CreateDomainStmt
			| CreateExternalStmt
			| CreateFdwStmt
			| CreateFileSpaceStmt
			| CreateForeignServerStmt
			| CreateForeignStmt
			| CreateFunctionStmt
			| CreateGroupStmt
			| CreateOpClassStmt
			| CreatePLangStmt
			| CreateQueueStmt
			| CreateSchemaStmt
			| CreateSeqStmt
			| CreateStmt
			| CreateTableSpaceStmt
			| CreateTrigStmt
			| CreateRoleStmt
			| CreateUserStmt
			| CreateUserMappingStmt
			| CreatedbStmt
			| DeallocateStmt
			| DeclareCursorStmt
			| DefineStmt
			| DeleteStmt
			| DropAssertStmt
			| DropCastStmt
			| DropFdwStmt
			| DropForeignServerStmt
			| DropGroupStmt
			| DropOpClassStmt
			| DropOwnedStmt
			| DropPLangStmt
			| DropQueueStmt
			| DropRuleStmt
			| DropStmt
			| DropTrigStmt
			| DropRoleStmt
			| DropUserMappingStmt
			| DropUserStmt
			| DropdbStmt
			| ExecuteStmt
			| ExplainStmt
			| FetchStmt
			| GrantStmt
			| GrantRoleStmt
			| IndexStmt
			| InsertStmt
			| ListenStmt
			| LoadStmt
			| LockStmt
			| NotifyStmt
			| PrepareStmt
			| ReassignOwnedStmt
			| ReindexStmt
			| RemoveAggrStmt
			| RemoveFuncStmt
			| RemoveOperStmt
			| RenameStmt
			| RevokeStmt
			| RevokeRoleStmt
			| RuleStmt
			| SelectStmt
			| TransactionStmt
			| TruncateStmt
			| UnlistenStmt
			| UpdateStmt
			| VacuumStmt
			| VariableResetStmt
			| VariableSetStmt
			| VariableShowStmt
			| ViewStmt
			| /*EMPTY*/
				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 * Create a new Postgres Resource Queue
 *
 *****************************************************************************/

size_unit: '%'     	{$$ = makeString("%"); }  |
		   KB	   	{$$ = makeString("kb"); } | 
		   MB 	   	{$$ = makeString("mb"); } |
		   GB 		{$$ = makeString("gb"); } |
		   TB 		{$$ = makeString("tb"); } |
		   PB 		{$$ = makeString("pb"); } 
		;

resqueue_attr_definition: 
			'(' resqueue_attr_def_list ')'		  
			{ 
				$$ = $2; 
			}
		;

resqueue_attr_def_list:  
			resqueue_attr_def_elem					
				{ 
					$$ = list_make1($1); 
				}
			| resqueue_attr_def_list ',' resqueue_attr_def_elem	
				{ 
					$$ = lappend($1, $3); 
				}
		;

resqueue_attr_def_elem:  
			ColLabel '=' Sconst
				{
					$$ = makeDefElem($1, (Node *)makeString($3));
				}
			| ColLabel '=' '(' IntegerOnly size_unit ',' IntegerOnly size_unit ')'
				{
					char valuestr[256];
					snprintf(valuestr, sizeof(valuestr), "(%ld%s,%ld%s)",
							 $4->val.ival, $5->val.str,
							 $7->val.ival, $8->val.str);
					char *permstr = palloc0(sizeof(char)*(strlen(valuestr)+1));
					strcpy(permstr, valuestr); 
					$$ = makeDefElem($1, (Node *)makeString(permstr));
				} 
			| ColLabel '=' IntegerOnly size_unit
				{
					char valuestr[256];
					/*snprintf(valuestr, sizeof(valuestr), "(0%s,%ld%s)",
							 $4->val.str,
							 $3->val.ival,
							 $4->val.str);*/
					snprintf(valuestr, sizeof(valuestr), "%ld%s",
												 $3->val.ival,
												 $4->val.str);
					char *permstr = palloc0(sizeof(char)*(strlen(valuestr)+1));
					strcpy(permstr, valuestr); 
					$$ = makeDefElem($1, (Node *)makeString(permstr));
				}
			| ColLabel '=' '(' IntegerOnly ',' IntegerOnly ')'
				{
					char valuestr[256];
					snprintf(valuestr, sizeof(valuestr), "(%ld,%ld)",
							 $4->val.ival,
							 $6->val.ival);
					char *permstr = palloc0(sizeof(char)*(strlen(valuestr)+1));
					strcpy(permstr, valuestr); 
					$$ = makeDefElem($1, (Node *)makeString(permstr));
				}
			| ColLabel '=' NumericOnly
				{
					/*char valuestr[256];
					snprintf(valuestr, sizeof(valuestr), "(0,%ld)",
							 $3->val.ival);
					snprintf(valuestr, sizeof(valuestr), "%ld",
												 $3->val.ival);
					char *permstr = palloc0(sizeof(char)*(strlen(valuestr)+1));
					strcpy(permstr, valuestr); 
					$$ = makeDefElem($1, (Node *)makeString(permstr));*/
					$$ = makeDefElem($1, (Node *)$3);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL);
				}
		;


CreateQueueStmt:
			CREATE RESOURCE QUEUE QueueId WITH resqueue_attr_definition
				{
					CreateQueueStmt *n = makeNode(CreateQueueStmt);
					DefElem			*def1 =
						makeDefElem(WITHLISTSTART_TAG,
									(Node *)makeInteger(TRUE));
					n->queue = $4;				/* Set queue name. 			*/
					n->options = list_concat(list_make1(def1), $6);
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * Alter a postgres Resource Queue
 *
 *****************************************************************************/

AlterQueueStmt:
			ALTER RESOURCE QUEUE QueueId WITH resqueue_attr_definition
				 {
					AlterQueueStmt *n    = makeNode(AlterQueueStmt);
					DefElem        *def1 = /* mark start of WITH items */
						makeDefElem(WITHLISTSTART_TAG, 
									(Node *)makeInteger(TRUE));
					DefElem        *def2 = /* mark start of WITHOUT items */
						makeDefElem(WITHOUTLISTSTART_TAG, 
									(Node *)makeInteger(TRUE));
					n->queue = $4;
					n->options = list_concat(list_make1(def1), $6); 
					n->options = lappend(n->options, def2); 
					$$ = (Node *)n;
				 }
			| ALTER RESOURCE QUEUE QueueId WITHOUT resqueue_attr_definition
				 {
					AlterQueueStmt *n    = makeNode(AlterQueueStmt);
					DefElem        *def1 = /* mark start of WITH items */
						makeDefElem(WITHLISTSTART_TAG, 
									(Node *)makeInteger(TRUE));
					DefElem        *def2 = /* mark start of WITHOUT items */
						makeDefElem(WITHOUTLISTSTART_TAG, 
									(Node *)makeInteger(TRUE));
					n->queue = $4;
					n->options = list_make1(def1); 
					n->options = list_concat(lappend(n->options, def2), $6); 
					$$ = (Node *)n;
				 }
			| ALTER RESOURCE QUEUE QueueId WITH resqueue_attr_definition 
			  WITHOUT definition
				 {
					AlterQueueStmt *n    = makeNode(AlterQueueStmt);
					DefElem        *def1 = /* mark start of WITH items */
						makeDefElem(WITHLISTSTART_TAG, 
									(Node *)makeInteger(TRUE));
					DefElem        *def2 = /* mark start of WITHOUT items */
						makeDefElem(WITHOUTLISTSTART_TAG, 
									(Node *)makeInteger(TRUE));
					n->queue = $4;
					n->options = list_concat(list_make1(def1), $6); 
					n->options = list_concat(lappend(n->options, def2), $8);
					$$ = (Node *)n;
				 }
		;

/*****************************************************************************
 *
 * Drop a postgres Resource Queue
 *
 *****************************************************************************/

DropQueueStmt:
			DROP RESOURCE QUEUE QueueId
				 {
					DropQueueStmt *n = makeNode(DropQueueStmt);
					n->queue = $4;
					$$ = (Node *)n;
				 }
		;

/*****************************************************************************
 *
 * Create a new Postgres DBMS role
 *
 *****************************************************************************/

CreateRoleStmt:
			CREATE ROLE RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_ROLE;
					n->role = $3;
					n->options = $5;
					n->roleOid = 0;
					$$ = (Node *)n;
				}
		;


opt_with:	WITH									{}
			| /*EMPTY*/								{}
		;

/*
 * Options for CREATE ROLE and ALTER ROLE (also used by CREATE/ALTER USER
 * for backwards compatibility).  Note: the only option required by SQL99
 * is "WITH ADMIN name".
 */
OptRoleList:
			OptRoleList OptRoleElem					{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

OptRoleElem:
			PASSWORD Sconst
				{
					$$ = makeDefElem("password",
									 (Node *)makeString($2));
				}
			| PASSWORD NULL_P
				{
					$$ = makeDefElem("password", NULL);
				}
			| ENCRYPTED PASSWORD Sconst
				{
					$$ = makeDefElem("encryptedPassword",
									 (Node *)makeString($3));
				}
			| UNENCRYPTED PASSWORD Sconst
				{
					$$ = makeDefElem("unencryptedPassword",
									 (Node *)makeString($3));
				}
			| SUPERUSER_P
				{
					$$ = makeDefElem("superuser", (Node *)makeInteger(TRUE));
				}
			| NOSUPERUSER
				{
					$$ = makeDefElem("superuser", (Node *)makeInteger(FALSE));
				}
			| INHERIT
				{
					$$ = makeDefElem("inherit", (Node *)makeInteger(TRUE));
				}
			| NOINHERIT
				{
					$$ = makeDefElem("inherit", (Node *)makeInteger(FALSE));
				}
			| CREATEDB
				{
					$$ = makeDefElem("createdb", (Node *)makeInteger(TRUE));
				}
			| NOCREATEDB
				{
					$$ = makeDefElem("createdb", (Node *)makeInteger(FALSE));
				}
			| CREATEROLE
				{
					$$ = makeDefElem("createrole", (Node *)makeInteger(TRUE));
				}
			| NOCREATEROLE
				{
					$$ = makeDefElem("createrole", (Node *)makeInteger(FALSE));
				}
			| CREATEUSER
				{
					/* For backwards compatibility, synonym for SUPERUSER */
					$$ = makeDefElem("superuser", (Node *)makeInteger(TRUE));
				}
			| NOCREATEUSER
				{
					$$ = makeDefElem("superuser", (Node *)makeInteger(FALSE));
				}
			| LOGIN_P
				{
					$$ = makeDefElem("canlogin", (Node *)makeInteger(TRUE));
				}
			| NOLOGIN_P
				{
					$$ = makeDefElem("canlogin", (Node *)makeInteger(FALSE));
				}
			| CONNECTION LIMIT SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($3));
				}
			| VALID UNTIL Sconst
				{
					$$ = makeDefElem("validUntil", (Node *)makeString($3));
				}
			| RESOURCE QUEUE any_name
				{
					$$ = makeDefElem("resourceQueue", (Node *)$3);
				}
		/*	Supported but not documented for roles, for use by ALTER GROUP. */
			| USER name_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2);
				}
		/* The following are not supported by ALTER ROLE/USER/GROUP */
			| SYSID Iconst
				{
					$$ = makeDefElem("sysid", (Node *)makeInteger($2));
				}
			| ADMIN name_list
				{
					$$ = makeDefElem("adminmembers", (Node *)$2);
				}
			| ROLE name_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2);
				}
			| IN_P ROLE name_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3);
				}
			| IN_P GROUP_P name_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3);
				}
			| CREATEEXTTABLE exttab_auth_list
				{
					$$ = makeDefElem("exttabauth", (Node *)$2);
				}
			| NOCREATEEXTTABLE exttab_auth_list
				{
					$$ = makeDefElem("exttabnoauth", (Node *)$2);
				}			
			| deny_login_role
				{
					$$ = makeDefElem("deny", (Node *)$1);
				}
		;

deny_login_role: DENY deny_interval { $$ = (Node *)$2; }
			| DENY deny_point { $$ = (Node *)$2; }
		;

deny_interval: BETWEEN deny_point AND deny_point
				{
					DenyLoginInterval *n = makeNode(DenyLoginInterval);
					n->start = (DenyLoginPoint *)$2;
					n->end = (DenyLoginPoint *)$4;
					$$ = (Node *)n;
				}
		;

deny_day_specifier: Sconst { $$ = (Node *)makeString($1); }
			| Iconst { $$ = (Node *)makeInteger($1); }
		;

deny_point: DAY_P deny_day_specifier opt_time
				{
					DenyLoginPoint *n = makeNode(DenyLoginPoint);
					n->day = (Value *)$2;
					n->time = (Value *)$3;
					$$ = (Node *)n;
				}
		;

opt_time: TIME Sconst { $$ = (Node *)makeString($2); }
		| /* nothing */ { $$ = NULL; }
		;

exttab_auth_list:
		'(' keyvalue_list ')'	{ $$ = $2; }
		| /*EMPTY*/				{ $$ = NIL; }
		;

keyvalue_list:
		keyvalue_pair						{ $$ = list_make1($1); }
		| keyvalue_list ',' keyvalue_pair	{ $$ = lappend($1, $3); }
		;

keyvalue_pair:
		ColLabel '=' Sconst
		{
			$$ = makeDefElem($1, (Node *)makeString($3));
		}
		;


/*****************************************************************************
 *
 * Create a new Postgres DBMS user (role with implied login ability)
 *
 *****************************************************************************/

CreateUserStmt:
			CREATE USER RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_USER;
					n->role = $3;
					n->options = $5;
					n->roleOid = 0;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql DBMS role
 *
 *****************************************************************************/

AlterRoleStmt:
			ALTER ROLE RoleId opt_with OptAlterRoleList
				 {
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					$$ = (Node *)n;
				 }
		;

AlterRoleSetStmt:
			ALTER ROLE RoleId SET set_rest
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
					n->role = $3;
					n->variable = $5->name;
					n->value = $5->args;
					$$ = (Node *)n;
				}
			| ALTER ROLE RoleId VariableResetStmt
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
					n->role = $3;
					n->variable = ((VariableResetStmt *)$4)->name;
					n->value = NIL;
					$$ = (Node *)n;
				}
		;

/* OptAlterRoleList is effectively OptRoleList with additional support for DROP DENY FOR. */
OptAlterRoleList:
            OptAlterRoleList OptAlterRoleElem       { $$ = lappend($1, $2); }
            | /*EMPTY*/                             { $$ = NIL; }
        ;

OptAlterRoleElem:
			OptRoleElem								{ $$ = $1; }
			| DROP DENY FOR deny_point				{ $$ = makeDefElem("drop_deny", $4); }



/*****************************************************************************
 *
 * Alter a postgresql DBMS user
 *
 *****************************************************************************/

AlterUserStmt:
			ALTER USER RoleId opt_with OptAlterRoleList
				 {
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					$$ = (Node *)n;
				 }
		;


AlterUserSetStmt:
			ALTER USER RoleId SET set_rest
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
					n->role = $3;
					n->variable = $5->name;
					n->value = $5->args;
					$$ = (Node *)n;
				}
			| ALTER USER RoleId VariableResetStmt
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
					n->role = $3;
					n->variable = ((VariableResetStmt *)$4)->name;
					n->value = NIL;
					$$ = (Node *)n;
				}
			;


/*****************************************************************************
 *
 * Drop a postgresql DBMS role
 *
 * XXX Ideally this would have CASCADE/RESTRICT options, but since a role
 * might own objects in multiple databases, there is presently no way to
 * implement either cascading or restricting.  Caveat DBA.
 *****************************************************************************/

DropRoleStmt:
			DROP ROLE name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP ROLE IF_P EXISTS name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = TRUE;
					n->roles = $5;
					$$ = (Node *)n;
				}
			;

/*****************************************************************************
 *
 * Drop a postgresql DBMS user
 *
 * XXX Ideally this would have CASCADE/RESTRICT options, but since a user
 * might own objects in multiple databases, there is presently no way to
 * implement either cascading or restricting.  Caveat DBA.
 *****************************************************************************/

DropUserStmt:
			DROP USER name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP USER IF_P EXISTS name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->roles = $5;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			;


/*****************************************************************************
 *
 * Create a postgresql group (role without login ability)
 *
 *****************************************************************************/

CreateGroupStmt:
			CREATE GROUP_P RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_GROUP;
					n->role = $3;
					n->options = $5;
					n->roleOid = 0;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql group
 *
 *****************************************************************************/

AlterGroupStmt:
			ALTER GROUP_P RoleId add_drop USER name_list
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = $4;
					n->options = list_make1(makeDefElem("rolemembers",
														(Node *)$6));
					$$ = (Node *)n;
				}
		;

add_drop:	ADD_P										{ $$ = +1; }
			| DROP									{ $$ = -1; }
		;


/*****************************************************************************
 *
 * Drop a postgresql group
 *
 * XXX see above notes about cascading DROP USER; groups have same problem.
 *****************************************************************************/

DropGroupStmt:
			DROP GROUP_P name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP GROUP_P IF_P EXISTS name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = TRUE;
					n->roles = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Manipulate a schema
 *
 *****************************************************************************/

CreateSchemaStmt:
			CREATE SCHEMA OptSchemaName AUTHORIZATION RoleId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* One can omit the schema name or the authorization id. */
					if ($3 != NULL)
						n->schemaname = $3;
					else
						n->schemaname = $5;
					n->authid = $5;
					n->schemaElts = $6;
					n->schemaOid = 0;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA ColId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* ...but not both */
					n->schemaname = $3;
					n->authid = NULL;
					n->schemaElts = $4;
					n->schemaOid = 0;
					$$ = (Node *)n;
				}
		;

OptSchemaName:
			ColId									{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

OptSchemaEltList:
			OptSchemaEltList schema_stmt			{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*
 *	schema_stmt are the ones that can show up inside a CREATE SCHEMA
 *	statement (in addition to by themselves).
 */
schema_stmt:
			CreateStmt
			| IndexStmt
			| CreateSeqStmt
			| CreateTrigStmt
			| GrantStmt
			| ViewStmt
		;


/*****************************************************************************
 *
 * Set PG internal variable
 *	  SET name TO 'var_value'
 * Include SQL92 syntax (thomas 1997-10-22):
 *	  SET TIME ZONE 'var_value'
 *
 *****************************************************************************/

VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			| SET LOCAL set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = true;
					$$ = (Node *) n;
				}
			| SET SESSION set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = false;
					$$ = (Node *) n;
				}
		;

set_rest:  var_name TO var_list_or_default
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name '=' var_list_or_default
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| TIME ZONE zone_value
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = "timezone";
					if ($3 != NULL)
						n->args = list_make1($3);
					$$ = n;
				}
			| TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = "TRANSACTION";
					n->args = $2;
					$$ = n;
				}
			| SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = "SESSION CHARACTERISTICS";
					n->args = $5;
					$$ = n;
				}
			| NAMES opt_encoding
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = "client_encoding";
					if ($2 != NULL)
						n->args = list_make1(makeStringConst($2, NULL, @2));
					$$ = n;
				}
			| ROLE ColId_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = "role";
					n->args = list_make1(makeStringConst($2, NULL, @2));
					$$ = n;
				}
			| SESSION AUTHORIZATION ColId_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = "session_authorization";
					n->args = list_make1(makeStringConst($3, NULL, @3));
					$$ = n;
				}
			| SESSION AUTHORIZATION DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->name = "session_authorization";
					n->args = NIL;
					$$ = n;
				}
		;

var_name:
			ColId								{ $$ = $1; }
			| var_name '.' ColId
				{
					int qLen = strlen($1);
					char* qualName = palloc(qLen + strlen($3) + 2);
					strcpy(qualName, $1);
					qualName[qLen] = '.';
					strcpy(qualName + qLen + 1, $3);
					$$ = qualName;
				}
		;

var_list_or_default:
			var_list								{ $$ = $1; }
			| DEFAULT								{ $$ = NIL; }
		;

var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;

var_value:	opt_boolean
				{ $$ = makeStringConst($1, NULL, @1); }
			| ColId_or_Sconst
				{ $$ = makeStringConst($1, NULL, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
		;

iso_level:	READ UNCOMMITTED						{ $$ = "read uncommitted"; }
			| READ COMMITTED						{ $$ = "read committed"; }
			| REPEATABLE READ						{ $$ = "repeatable read"; }
			| SERIALIZABLE							{ $$ = "serializable"; }
		;

opt_boolean:
			TRUE_P									{ $$ = "true"; }
			| FALSE_P								{ $$ = "false"; }
			| ON									{ $$ = "on"; }
			| OFF									{ $$ = "off"; }
		;

/* Timezone values can be:
 * - a string such as 'pst8pdt'
 * - an identifier such as "pst8pdt"
 * - an integer or floating point number
 * - a time interval per SQL99
 * ColId gives reduce/reduce errors against ConstInterval and LOCAL,
 * so use IDENT and reject anything which is a reserved word.
 */
zone_value:
			Sconst
				{
					$$ = makeStringConst($1, NULL, @1);
				}
			| IDENT
				{
					$$ = makeStringConst($1, NULL, @1);
				}
			| ConstInterval Sconst opt_interval
				{
					A_Const *n = (A_Const *) makeStringConst($2, $1, @2);
					if ($3 != INTERVAL_FULL_RANGE)
					{
						if (($3 & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 scanner_errposition(@3)));
						n->typname->typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, $3);
					}
					$$ = (Node *)n;
				}
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					A_Const *n = (A_Const *) makeStringConst($5, $1, @5);
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("INTERVAL(%d) precision must not be negative", $3),
								 scanner_errposition(@3)));
					if ($3 > MAX_INTERVAL_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("INTERVAL(%d) precision reduced to maximum allowed, %d",
										$3, MAX_INTERVAL_PRECISION)));
						$3 = MAX_INTERVAL_PRECISION;
					}

					if (($6 != INTERVAL_FULL_RANGE)
						&& (($6 & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("time zone interval must be HOUR or HOUR TO MINUTE")));

					n->typname->typmod = INTERVAL_TYPMOD($3, $6);

					$$ = (Node *)n;
				}
			| NumericOnly							{ $$ = makeAConst($1, @1); }
			| DEFAULT								{ $$ = NULL; }
			| LOCAL									{ $$ = NULL; }
		;

opt_encoding:
			Sconst									{ $$ = $1; }
			| DEFAULT								{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ColId_or_Sconst:
			ColId									{ $$ = $1; }
			| SCONST								{ $$ = $1; }
		;


VariableShowStmt:
			SHOW var_name
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = $2;
					$$ = (Node *) n;
				}
			| SHOW TIME ZONE
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "timezone";
					$$ = (Node *) n;
				}
			| SHOW TRANSACTION ISOLATION LEVEL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "transaction_isolation";
					$$ = (Node *) n;
				}
			| SHOW SESSION AUTHORIZATION
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "session_authorization";
					$$ = (Node *) n;
				}
			| SHOW ALL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "all";
					$$ = (Node *) n;
				}
		;

VariableResetStmt:
			RESET var_name
				{
					VariableResetStmt *n = makeNode(VariableResetStmt);
					n->name = $2;
					$$ = (Node *) n;
				}
			| RESET TIME ZONE
				{
					VariableResetStmt *n = makeNode(VariableResetStmt);
					n->name = "timezone";
					$$ = (Node *) n;
				}
			| RESET TRANSACTION ISOLATION LEVEL
				{
					VariableResetStmt *n = makeNode(VariableResetStmt);
					n->name = "transaction_isolation";
					$$ = (Node *) n;
				}
			| RESET SESSION AUTHORIZATION
				{
					VariableResetStmt *n = makeNode(VariableResetStmt);
					n->name = "session_authorization";
					$$ = (Node *) n;
				}
			| RESET ALL
				{
					VariableResetStmt *n = makeNode(VariableResetStmt);
					n->name = "all";
					$$ = (Node *) n;
				}
		;


ConstraintsSetStmt:
			SET CONSTRAINTS constraints_set_list constraints_set_mode
				{
					ConstraintsSetStmt *n = makeNode(ConstraintsSetStmt);
					n->constraints = $3;
					n->deferred    = $4;
					$$ = (Node *) n;
				}
		;

constraints_set_list:
			ALL										{ $$ = NIL; }
			| qualified_name_list					{ $$ = $1; }
		;

constraints_set_mode:
			DEFERRED								{ $$ = TRUE; }
			| IMMEDIATE								{ $$ = FALSE; }
		;


/*
 * Checkpoint statement
 */
CheckPointStmt:
			CHECKPOINT
				{
					CheckPointStmt *n = makeNode(CheckPointStmt);
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *	ALTER [ TABLE | INDEX ] variations
 *
 *****************************************************************************/

AlterTableStmt:
			ALTER TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_TABLE;
					$$ = (Node *)n;
				}
		|	ALTER EXTERNAL TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $4;
					n->cmds = $5;
					n->relkind = OBJECT_EXTTABLE;
					$$ = (Node *)n;
				}
		|	ALTER FOREIGN TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->relation = $4;
					n->cmds = $5;
					n->relkind = OBJECT_FOREIGNTABLE;
					$$ = (Node *)n;
				}
		|	ALTER INDEX relation_expr alter_rel_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_INDEX;
					$$ = (Node *)n;
				}
		;

alter_table_cmds:
			alter_table_cmd							{ $$ = list_make1($1); }
			| alter_table_cmds ',' alter_table_cmd	{ $$ = lappend($1, $3); }
		;

/* Subcommands that are for ALTER TABLE only */
alter_table_cmd:
			/* ALTER TABLE <relation> ADD [COLUMN] <coldef> */
			ADD_P opt_column columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT} */
			| ALTER opt_column ColId alter_column_default
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ColumnDefault;
					n->name = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> ALTER [COLUMN] <colname> DROP NOT NULL */
			| ALTER opt_column ColId DROP NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> ALTER [COLUMN] <colname> SET NOT NULL */
			| ALTER opt_column ColId SET NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> ALTER [COLUMN] <colname> SET STATISTICS <IntegerOnly> */
			| ALTER opt_column ColId SET STATISTICS IntegerOnly
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStatistics;
					n->name = $3;
					n->def = (Node *) $6;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> ALTER [COLUMN] <colname> SET STORAGE <storagemode> */
			| ALTER opt_column ColId SET STORAGE ColId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStorage;
					n->name = $3;
					n->def = (Node *) makeString($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> DROP [COLUMN] <colname> [RESTRICT|CASCADE] */
			| DROP opt_column ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
			/*
			 * ALTER TABLE <relation> ALTER [COLUMN] <colname> TYPE <typename>
			 *		[ USING <expression> ]
			 */
			| ALTER opt_column ColId TYPE_P Typename alter_using
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) $5;
					n->transform = $6;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> ADD CONSTRAINT ... */
			| ADD_P TableConstraint
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddConstraint;
					n->def = $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <relation> SET WITHOUT OIDS  */
			| SET WITHOUT OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> CLUSTER ON <indexname> */
			| CLUSTER ON name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ClusterOn;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT CLUSTER */
			| SET WITHOUT CLUSTER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropCluster;
					n->name = NULL;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER <trig> */
			| ENABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER ALL */
			| ENABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER USER */
			| ENABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER <trig> */
			| DISABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER ALL */
			| DISABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER USER */
			| DISABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> INHERIT <parent> [( column_list )] */
			| INHERIT qualified_name opt_inherited_column_list
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddInherit;
					n->def = (Node *)list_make2($2, $3);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NO INHERIT <parent> */
			| NO INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropInherit;
					n->def = (Node *) $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET [WITH] [DISTRIBUTED BY] */
			/* distro only */
			| SET DistributedBy
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetDistributedBy;
					n->def = (Node *) list_make2(NULL, $2);
					$$ = (Node *)n;
				}
			/* storage and distro */
			| SET WITH definition DistributedBy
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetDistributedBy;
					n->def = (Node *) list_make2($3, $4);
					$$ = (Node *)n;
				}
			/* storage only */
			| SET WITH definition
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetDistributedBy;
					n->def = (Node *) list_make2($3, NULL);
					$$ = (Node *)n;
				}
			| alter_table_partition_cmd
				{
					$$ = $1;
				}
			| alter_rel_cmd
				{
					$$ = $1;
				}
		;

opt_table_partition_split_into: 
			INTO '(' 
            alter_table_partition_id_spec_with_opt_default ','
            alter_table_partition_id_spec_with_opt_default ')'	
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
                    pc->partid = (Node *)$3;
                    pc->arg1 = (Node *)$5;
                    pc->arg2 = NULL;
                    pc->location = @5;
					$$ = (Node *)pc;
                }
			| /*EMPTY*/						{ $$ = NULL; /* default */ }
		;

opt_table_partition_merge_into: 
			INTO 
            alter_table_partition_id_spec_with_opt_default
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
                    pc->partid = (Node *)$2;
                    pc->arg1 = NULL;
                    pc->arg2 = NULL;
                    pc->location = @2;
					$$ = (Node *)pc;
                }

			| /*EMPTY*/						{ $$ = NULL; /* default */ }
		;

table_partition_modify:
			TabPartitionBoundarySpecStart
            OptTabPartitionBoundarySpecEnd
            OptTabPartitionBoundarySpecEvery  
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd  *pc = makeNode(AlterPartitionCmd);
                    PartitionBoundSpec *bs = makeNode(PartitionBoundSpec); 
                    PartitionElem      *n  = makeNode(PartitionElem); 

                    n->partName  = NULL;
                    n->boundSpec = (Node *)bs;
                    n->subSpec   = NULL;
                    n->location  = @1;
                    n->isDefault = 0;
                    n->storeAttr = NULL;
                    n->AddPartDesc = NULL;

                    bs->partStart = $1;
                    bs->partEnd   = $2;
                    bs->partEvery = $3;
                    bs->everyGenList = NIL; 
                    bs->pWithTnameStr = NULL;
                    bs->location  = @1;

                    pc->partid = NULL;
                    pc->arg1 = (Node *)makeDefElem("START", NULL);
                    pc->arg2 = (Node *)n;
                    pc->location = @1;
					$$ = (Node *)pc;
                }
			| TabPartitionBoundarySpecEnd
              OptTabPartitionBoundarySpecEvery	
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd  *pc = makeNode(AlterPartitionCmd);
                    PartitionBoundSpec *bs = makeNode(PartitionBoundSpec); 
                    PartitionElem      *n  = makeNode(PartitionElem); 

                    n->partName  = NULL;
                    n->boundSpec = (Node *)bs;
                    n->subSpec   = NULL;
                    n->location  = @1;
                    n->isDefault = 0;
                    n->storeAttr = NULL;
                    n->AddPartDesc = NULL;

                    bs->partStart = NULL;
                    bs->partEnd   = $1;
                    bs->partEvery = $2;
                    bs->everyGenList = NIL; 
                    bs->pWithTnameStr = NULL;
                    bs->location  = @1;

                    pc->partid = NULL;
                    pc->arg1 = (Node *)makeDefElem("END", NULL);
                    pc->arg2 = (Node *)n;
                    pc->location = @1;
					$$ = (Node *)pc;
                }
			| add_drop part_values_clause
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd   *pc = makeNode(AlterPartitionCmd);
                    PartitionValuesSpec *vs = makeNode(PartitionValuesSpec); 
                    PartitionElem       *n  = makeNode(PartitionElem); 

                    n->partName  = NULL;
                    n->boundSpec = (Node *)vs;
                    n->subSpec   = NULL;
                    n->location  = @1;
                    n->isDefault = 0;
                    n->storeAttr = NULL;
                    n->AddPartDesc = NULL;

                    vs->partValues = $2;
                    vs->location  = @2;

                    pc->partid = NULL;
                    if (1 == $1)
                        pc->arg1 = (Node *)makeDefElem("ADD", NULL);
                    else
                        pc->arg1 = (Node *)makeDefElem("DROP", NULL);
                    pc->arg2 = (Node *)n;
                    pc->location = @2;
					$$ = (Node *)pc;
                }
		;

opt_table_partition_exchange_validate: 
			WITH VALIDATION						{ $$ = +1; }
			| WITHOUT VALIDATION				{ $$ = +0; }
			| /*EMPTY*/							{ $$ = +1; /* default */ }
		;

alter_table_partition_id_spec: 
			PartitionColId
				{
					AlterPartitionId *n = makeNode(AlterPartitionId);
					n->idtype = AT_AP_IDName;
                    n->partiddef = (Node *)makeString($1);
                    n->location  = @1;
					$$ = (Node *)n;
				}
            | FOR 
            '(' TabPartitionBoundarySpecValList ')'	
				{
					AlterPartitionId *n = makeNode(AlterPartitionId);
					n->idtype = AT_AP_IDValue;
                    n->partiddef = (Node *)$3;
                    n->location  = @3;
					$$ = (Node *)n;
				}
            | FOR '(' function_name '(' NumericOnly ')' ')'	
				{
					AlterPartitionId *n = makeNode(AlterPartitionId);
					n->idtype = AT_AP_IDRank;
                    n->partiddef = (Node *)$5;
                    n->location  = @5;

                    /* allow RANK only */
					if (!(strcmp($3, "rank") == 0))
                        yyerror("syntax error");

					$$ = (Node *)n;
				}
		;

alter_table_partition_id_spec_with_opt_default:
			PARTITION alter_table_partition_id_spec
				{
					AlterPartitionId *n = (AlterPartitionId*)$2;
                    $$ = (Node *)n;
				}
			| DEFAULT PARTITION alter_table_partition_id_spec
				{
					ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("Cannot specify a name, rank, "
                                    "or value for a DEFAULT partition "
                                    "in this context")));
				}
			| DEFAULT PARTITION 
				{
					AlterPartitionId *n = makeNode(AlterPartitionId);
					n->idtype = AT_AP_IDDefault;
                    n->partiddef = NULL;
                    n->location  = @1;
					$$ = (Node *)n;
				}
		;

alter_table_partition_cmd:
			ADD_P PARTITION 
            OptTabPartitionBoundarySpec
 			OptTabPartitionStorageAttr
			OptTabSubPartitionSpec
           
				{
					AlterPartitionId  *pid   = makeNode(AlterPartitionId);
					AlterPartitionCmd *pc    = makeNode(AlterPartitionCmd);
					AlterPartitionCmd *pc2   = makeNode(AlterPartitionCmd);
					AlterTableCmd     *n     = makeNode(AlterTableCmd);
                    Node              *ct    = makeAddPartitionCreateStmt($4, $5);
                    PartitionElem     *pelem = makeNode(PartitionElem); 

                    pid->idtype = AT_AP_IDNone;
                    pid->location = @3;
                    pid->partiddef = NULL;

                    pc->partid = (Node *)pid;

                    pelem->partName  = NULL;
                    pelem->boundSpec = $3;
                    pelem->subSpec   = $5;
                    pelem->location  = @3;
                    pelem->isDefault = 0;
                    pelem->storeAttr = $4;
                    pelem->AddPartDesc = NULL;

                    pc2->arg1 = (Node *)pelem;
                    pc2->arg2 = (Node *)list_make1(ct);
                    pc2->location = @3;

                    pc->arg1 = (Node *)makeInteger(FALSE); /* not default */
                    pc->arg2 = (Node *)pc2;
                    pc->location = @3;

					n->subtype = AT_PartAdd;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| ADD_P DEFAULT PARTITION 
            alter_table_partition_id_spec 
            OptTabPartitionBoundarySpec
            OptTabPartitionStorageAttr
			OptTabSubPartitionSpec 
				{
					AlterPartitionId  *pid   = (AlterPartitionId *)$4;
					AlterPartitionCmd *pc    = makeNode(AlterPartitionCmd);
					AlterPartitionCmd *pc2   = makeNode(AlterPartitionCmd);
					AlterTableCmd     *n     = makeNode(AlterTableCmd);
                    Node              *ct    = makeAddPartitionCreateStmt($6, $7);
                    PartitionElem     *pelem = makeNode(PartitionElem); 

                    if (pid->idtype != AT_AP_IDName)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("Can only ADD a partition by name"),
								 errOmitLocation(true)));

                    pc->partid = (Node *)pid;

                    pelem->partName  = NULL;
                    pelem->boundSpec = $5;
                    pelem->subSpec   = $7;
                    pelem->location  = @5;
                    pelem->isDefault = true;
                    pelem->storeAttr = $6;
                    pelem->AddPartDesc = NULL;

                    pc2->arg1 = (Node *)pelem;
                    pc2->arg2 = (Node *)list_make1(ct);
                    pc2->location = @5;

                    pc->arg1 = (Node *)makeInteger(true);
                    pc->arg2 = (Node *)pc2;
                    pc->location = @5;

					n->subtype = AT_PartAdd;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| ADD_P PARTITION 
            alter_table_partition_id_spec 
            OptTabPartitionBoundarySpec
            OptTabPartitionStorageAttr
			OptTabSubPartitionSpec 
				{
					AlterPartitionId  *pid   = (AlterPartitionId *)$3;
					AlterPartitionCmd *pc    = makeNode(AlterPartitionCmd);
					AlterPartitionCmd *pc2   = makeNode(AlterPartitionCmd);
					AlterTableCmd     *n     = makeNode(AlterTableCmd);
                    Node              *ct    = makeAddPartitionCreateStmt($5, 
																		  $6);
                    PartitionElem     *pelem = makeNode(PartitionElem); 

                    if (pid->idtype != AT_AP_IDName)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("Can only ADD a partition by name")));

                    pc->partid = (Node *)pid;

                    pelem->partName  = NULL;
                    pelem->boundSpec = $4;
                    pelem->subSpec   = $6;
                    pelem->location  = @4;
                    pelem->isDefault = false;
                    pelem->storeAttr = $5;
                    pelem->AddPartDesc = NULL;

                    pc2->arg1 = (Node *)pelem;
                    pc2->arg2 = (Node *)list_make1(ct);
                    pc2->location = @4;

                    pc->arg1 = (Node *)makeInteger(false);
                    pc->arg2 = (Node *)pc2;
                    pc->location = @4;

					n->subtype = AT_PartAdd;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| ALTER 
            alter_table_partition_id_spec_with_opt_default
            alter_table_cmd
				{
                    /* NOTE: only allow a subset of valid ALTER TABLE
                       cmds for partitions.
                    */

					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    pc->partid = (Node *)$2;
                    pc->arg1 = (Node *)$3;
                    pc->arg2 = NULL;
                    pc->location = @3;

					n->subtype = AT_PartAlter;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| partition_coalesce_keyword PARTITION 	 
				{
					AlterPartitionId  *pid = makeNode(AlterPartitionId);
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    pid->idtype = AT_AP_IDNone;
                    pid->location = @2;
                    pid->partiddef = NULL;

                    pc->partid = (Node *)pid;
                    pc->arg1 = NULL;
                    pc->arg2 = NULL;
                    pc->location = @2;

					n->subtype = AT_PartCoalesce;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| partition_coalesce_keyword PARTITION 
            alter_table_partition_id_spec 
				{
					AlterPartitionId *pid = (AlterPartitionId *)$3;
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    if (pid->idtype != AT_AP_IDName)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("Can only COALESCE a partition by name")));

                    pc->partid = (Node *)pid;
                    pc->arg1 = NULL;
                    pc->arg2 = NULL;
                    pc->location = @3;

					n->subtype = AT_PartCoalesce;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| DROP PARTITION IF_P EXISTS 
            alter_table_partition_id_spec	 
            opt_drop_behavior
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);
					DropStmt *ds = makeNode(DropStmt);

					ds->missing_ok = TRUE;
					ds->behavior = $6;

                    /* 
                       build an (incomplete) drop statement for arg1: 
                       fill in the rest after the partition id spec is
                       validated
                    */

                    pc->partid = (Node *)$5;
                    pc->arg1 = (Node *)ds;
                    pc->arg2 = NULL;
                    pc->location = @5;

					n->subtype = AT_PartDrop;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| DROP DEFAULT PARTITION IF_P EXISTS 
            opt_drop_behavior
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);
					DropStmt *ds = makeNode(DropStmt);
					AlterPartitionId *pid = makeNode(AlterPartitionId);

					pid->idtype = AT_AP_IDDefault;
                    pid->partiddef = NULL;
                    pid->location  = @2;

					ds->missing_ok = TRUE;
					ds->behavior = $6;

                    /* 
                       build an (incomplete) drop statement for arg1: 
                       fill in the rest after the partition id spec is
                       validated
                    */

                    pc->partid = (Node *)pid;
                    pc->arg1 = (Node *)ds;
                    pc->arg2 = NULL;
                    pc->location = @3;

					n->subtype = AT_PartDrop;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| DROP 
            alter_table_partition_id_spec_with_opt_default
            opt_drop_behavior
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);
					DropStmt *ds = makeNode(DropStmt);

					ds->missing_ok = FALSE;
					ds->behavior = $3;

                    /* 
                       build an (incomplete) drop statement for arg1: 
                       fill in the rest after the partition id spec is
                       validated
                    */

                    pc->partid = (Node *)$2;
                    pc->arg1 = (Node *)ds;
                    pc->arg2 = NULL;
                    pc->location = @2;

					n->subtype = AT_PartDrop;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| DROP PARTITION 
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);
					DropStmt *ds = makeNode(DropStmt);
					AlterPartitionId *pid = makeNode(AlterPartitionId);

					ds->missing_ok = FALSE;
					ds->behavior = DROP_RESTRICT; /* default */ 

                    /* 
                       build an (incomplete) drop statement for arg1: 
                       fill in the rest after the partition id spec is
                       validated
                    */

                    /* just try to drop the first partition if not specified */
					pid->idtype = AT_AP_IDNone;
                    pid->location  = @2;

                    pc->partid = (Node *)pid;
                    pc->arg1 = (Node *)ds;
                    pc->arg2 = NULL;
                    pc->location = @2;

					n->subtype = AT_PartDrop;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| EXCHANGE 
            alter_table_partition_id_spec_with_opt_default 
            WITH TABLE qualified_name
            opt_table_partition_exchange_validate	
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterPartitionCmd *pc2 = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    pc->partid = (Node *)$2;
                    pc->arg1 = (Node *)$5;
                    pc->arg2 = (Node *)pc2;
                    pc2->arg1 = (Node *)makeInteger($6);
                    pc->location = @5;

					n->subtype = AT_PartExchange;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| MERGE 
            alter_table_partition_id_spec_with_opt_default ','
            alter_table_partition_id_spec_with_opt_default
            opt_table_partition_merge_into	
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    if (!gp_enable_hash_partitioned_tables)
                    {
                        yyerror("syntax error");
                    }

                    pc->partid = (Node *)$2;
                    pc->arg1 = (Node *)$4;
                    pc->arg2 = (Node *)$5;
                    pc->location = @4;

					n->subtype = AT_PartMerge;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| MODIFY 
            alter_table_partition_id_spec_with_opt_default
            table_partition_modify
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    if (!gp_enable_hash_partitioned_tables)
                    {
                        yyerror("syntax error");
                    }

                    pc->partid = (Node *)$2;
                    pc->arg1 = (Node *)$3;
                    pc->arg2 = NULL;
                    pc->location = @3;

					n->subtype = AT_PartModify;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| RENAME 
            alter_table_partition_id_spec_with_opt_default TO IDENT	
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    pc->partid = (Node *)$2;
                    pc->arg1 = (Node *)makeString($4);
                    pc->arg2 = NULL;
                    pc->location = @4;

					n->subtype = AT_PartRename;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| SET SUBPARTITION TEMPLATE
            '(' TabSubPartitionElemList ')' 
				{
					AlterPartitionId  *pid   = makeNode(AlterPartitionId);
					AlterPartitionCmd *pc    = makeNode(AlterPartitionCmd);
					AlterPartitionCmd *pc2   = makeNode(AlterPartitionCmd);
					AlterTableCmd     *n     = makeNode(AlterTableCmd);
                    Node              *ct    = NULL;
                    PartitionElem     *pelem = makeNode(PartitionElem); 
					PartitionSpec	  *ps    = makeNode(PartitionSpec); 

					/* treat this case as similar to ADD PARTITION */

                    pid->idtype    = AT_AP_IDName;
                    pid->location  = @3;
                    pid->partiddef = 
						(Node *)makeString("subpartition_template");

                    pc->partid = (Node *)pid;

					/* build a subpartition spec and add it to CREATE TABLE */
					ps->partElem   = $5; 
					ps->subSpec	   = NULL;
					ps->istemplate = true;
					ps->location   = @4;

					ct = makeAddPartitionCreateStmt(NULL,
													(Node *)ps);

                    pelem->partName  = NULL;
                    pelem->boundSpec = NULL;
                    pelem->subSpec   = (Node *)ps;
                    pelem->location  = @4;
					/*
                    pelem->isDefault = false;
					*/
                    pelem->isDefault = true;
                    pelem->storeAttr = NULL;
                    pelem->AddPartDesc = NULL;

					/* a little (temporary?) syntax check on templates */
					if (ps->partElem)
					{
						List *elems;
						ListCell *lc;
						Assert(IsA(ps->partElem, List));

						elems = (List *)ps->partElem;
						foreach(lc, elems)
						{
							PartitionElem *e = lfirst(lc);

							if (!IsA(e, PartitionElem))
								continue;

							if (e->subSpec)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("template cannot contain "
												"specification for child "
												"partition")));
						}
					}

                    pc2->arg1 = (Node *)pelem;
                    pc2->arg2 = (Node *)list_make1(ct);
                    pc2->location = @5;

					/*
                    pc->arg1 = (Node *)makeInteger(false);
					*/
                    pc->arg1 = (Node *)makeInteger(true);
                    pc->arg2 = (Node *)pc2;
                    pc->location = @5;

					n->subtype = AT_PartSetTemplate;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| SET SUBPARTITION TEMPLATE
            '('  ')' 
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    pc->partid = NULL; 
                    pc->arg1 = NULL;
                    pc->arg2 = NULL;
                    pc->location = @4;

					n->subtype = AT_PartSetTemplate;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| SPLIT 
            DEFAULT PARTITION TabPartitionBoundarySpecStart
            TabPartitionBoundarySpecEnd
            opt_table_partition_split_into	
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);
					AlterPartitionId *pid = makeNode(AlterPartitionId);

					pid->idtype = AT_AP_IDDefault;
                    pid->partiddef = NULL;
                    pid->location  = @2;

                    pc->partid = (Node *)pid;
                    pc->arg1 = (Node *)list_make2($4, $5);
                    pc->arg2 = (Node *)$6;
                    pc->location = @5;

					n->subtype = AT_PartSplit;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| SPLIT 
            alter_table_partition_id_spec_with_opt_default AT 
            '(' part_values_or_spec_list ')'	
            opt_table_partition_split_into	
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);

                    pc->partid = (Node *)$2;

					/* 
					 * The first element of the list is only defined if
					 * we're doing default splits for range partitioning.
				 	 */
                    pc->arg1 = (Node *)list_make2(NULL, $5);
                    pc->arg2 = (Node *)$7;
                    pc->location = @5;

					n->subtype = AT_PartSplit;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
			| TRUNCATE 
            alter_table_partition_id_spec_with_opt_default
            opt_drop_behavior
				{
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
					AlterTableCmd *n = makeNode(AlterTableCmd);
					TruncateStmt *ts = makeNode(TruncateStmt);

                    /* 
                       build an (incomplete) truncate statement for arg1: 
                       fill in the rest after the partition id spec is
                       validated
                    */
					ts->relations = NULL;
					ts->behavior = $3;

                    pc->partid = (Node *)$2;
                    pc->arg1 = (Node *)ts;
                    pc->arg2 = NULL;
                    pc->location = @2;

					n->subtype = AT_PartTruncate;
					n->def = (Node *)pc;
					$$ = (Node *)n;
				}
		;

alter_rel_cmds:
			alter_rel_cmd							{ $$ = list_make1($1); }
			| alter_rel_cmds ',' alter_rel_cmd		{ $$ = lappend($1, $3); }
		;

/* Subcommands that are for ALTER TABLE or ALTER INDEX */
alter_rel_cmd:
			/* ALTER [TABLE|INDEX] <name> OWNER TO RoleId */
			OWNER TO RoleId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ChangeOwner;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER [TABLE|INDEX] <name> SET TABLESPACE <tablespacename> */
			| SET TABLESPACE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetTableSpace;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER [TABLE|INDEX] <name> SET (...) */
			| SET definition
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			/* ALTER [TABLE|INDEX] <name> RESET (...) */
			| RESET definition
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
		;

alter_column_default:
			SET DEFAULT a_expr
				{
					/* Treat SET DEFAULT NULL the same as DROP DEFAULT */
					if (exprIsNullConstant($3))
						$$ = NULL;
					else
						$$ = $3;
				}
			| DROP DEFAULT				{ $$ = NULL; }
		;

opt_drop_behavior:
			CASCADE						{ $$ = DROP_CASCADE; }
			| RESTRICT					{ $$ = DROP_RESTRICT; }
			| /*EMPTY*/					{ $$ = DROP_RESTRICT; /* default */ }
		;

alter_using:
			USING a_expr				{ $$ = $2; }
			| /*EMPTY*/					{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				close <portalname>
 *
 *****************************************************************************/

ClosePortalStmt:
			CLOSE name
				{
					ClosePortalStmt *n = makeNode(ClosePortalStmt);
					n->portalname = $2;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				COPY relname ['(' columnList ')'] FROM/TO file [WITH options]
 *
 *				BINARY, OIDS, and DELIMITERS kept in old locations
 *				for backward compatibility.  2002-06-18
 *
 *				COPY ( SELECT ... ) TO file [WITH options]
 *				This form doesn't have the backwards-compatible option
 *				syntax.
 *
 *****************************************************************************/

CopyStmt:	COPY opt_binary qualified_name opt_column_list opt_oids
			copy_from copy_file_name copy_delimiter opt_with copy_opt_list
			OptSingleRowErrorHandling
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = $3;
					n->query = NULL;
					n->attlist = $4;
					n->is_from = $6;
					n->filename = $7;
					n->sreh = $11;
					n->partitions = NULL;
					n->ao_segnos = NIL;
					n->options = NIL;
					n->err_aosegnos = NIL;
					
					/* Concatenate user-supplied flags */
					if ($2)
						n->options = lappend(n->options, $2);
					if ($5)
						n->options = lappend(n->options, $5);
					if ($8)
						n->options = lappend(n->options, $8);
					if ($10)
						n->options = list_concat(n->options, $10);
					$$ = (Node *)n;
				}
			| COPY select_with_parens TO copy_file_name opt_with
			  copy_opt_list
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = NULL;
					n->query = (Query *) $2;
					n->attlist = NIL;
					n->is_from = false;
					n->filename = $4;
					n->options = $6;
					n->partitions = NULL;
					n->ao_segnos = NIL;
					$$ = (Node *)n;
				}
		;

copy_from:
			FROM									{ $$ = TRUE; }
			| TO									{ $$ = FALSE; }
		;

/*
 * copy_file_name NULL indicates stdio is used. Whether stdin or stdout is
 * used depends on the direction. (It really doesn't make sense to copy from
 * stdout. We silently correct the "typo".)		 - AY 9/94
 */
copy_file_name:
			Sconst									{ $$ = $1; }
			| STDIN									{ $$ = NULL; }
			| STDOUT								{ $$ = NULL; }
		;



copy_opt_list:
			copy_opt_list copy_opt_item				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;


copy_opt_item:
			BINARY
				{
					$$ = makeDefElem("binary", (Node *)makeInteger(TRUE));
				}
			| OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE));
				}
			| DELIMITER opt_as Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3));
				}
			| NULL_P opt_as Sconst
				{
					$$ = makeDefElem("null", (Node *)makeString($3));
				}
			| CSV
				{
					$$ = makeDefElem("csv", (Node *)makeInteger(TRUE));
				}
			| HEADER_P
				{
					$$ = makeDefElem("header", (Node *)makeInteger(TRUE));
				}
			| QUOTE opt_as Sconst
				{
					$$ = makeDefElem("quote", (Node *)makeString($3));
				}
			| ESCAPE opt_as Sconst
				{
					$$ = makeDefElem("escape", (Node *)makeString($3));
				}
			| FORCE QUOTE columnList
				{
					$$ = makeDefElem("force_quote", (Node *)$3);
				}
			| FORCE NOT NULL_P columnList
				{
					$$ = makeDefElem("force_notnull", (Node *)$4);
				}
			| FILL MISSING FIELDS
				{
					$$ = makeDefElem("fill_missing_fields", (Node *)makeInteger(TRUE));
				}
			| NEWLINE opt_as Sconst
				{
					$$ = makeDefElem("newline", (Node *)makeString($3));
				}	
		;

/* The following exist for backward compatibility */

opt_binary:
			BINARY
				{
					$$ = makeDefElem("binary", (Node *)makeInteger(TRUE));
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_oids:
			WITH OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE));
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

copy_delimiter:
			/* USING DELIMITERS kept for backward compatibility. 2002-06-15 */
			opt_using DELIMITERS Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3));
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_using:
			USING									{}
			| /*EMPTY*/								{}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 *****************************************************************************/

CreateStmt:	CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptWith OnCommitOption OptTableSpace OptDistributedBy 
			OptTabPartitionBy
				{
					CreateStmt *n = makeNode(CreateStmt);
					$4->istemp = $2;
					n->relation = $4;
					n->tableElts = $6;
					n->inhRelations = $8;
					n->constraints = NIL;
					n->options = $9;
					n->oncommit = $10;
					n->tablespacename = $11;
					n->distributedBy = $12;
					n->partitionBy = $13;
					n->oidInfo.relOid = 0;
					n->oidInfo.comptypeOid = 0;
					n->oidInfo.toastOid = 0;
					n->oidInfo.toastIndexOid = 0;
					n->oidInfo.toastComptypeOid = 0;
					n->relKind = RELKIND_RELATION;
					n->policy = 0;
					n->postCreate = NULL;
					
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE qualified_name OF qualified_name
			'(' OptTableElementList ')' OptWith OnCommitOption OptTableSpace OptDistributedBy OptTabPartitionBy
				{
					/* SQL99 CREATE TABLE OF <UDT> (cols) seems to be satisfied
					 * by our inheritance capabilities. Let's try it...
					 */
					CreateStmt *n = makeNode(CreateStmt);
					$4->istemp = $2;
					n->relation = $4;
					n->tableElts = $8;
					n->inhRelations = list_make1($6);
					n->constraints = NIL;
					n->options = $10;
					n->oncommit = $11;
					n->tablespacename = $12;
					n->distributedBy = $13;
					n->partitionBy = $14;
					n->oidInfo.relOid = 0;
					n->oidInfo.comptypeOid = 0;
					n->oidInfo.toastOid = 0;
					n->oidInfo.toastIndexOid = 0;
					n->oidInfo.toastComptypeOid = 0;
					n->relKind = RELKIND_RELATION;
					n->policy = 0;
                    n->postCreate = NULL;
					
					$$ = (Node *)n;
				}
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options; since we have no modules
 * the LOCAL keyword is really meaningless.
 */
OptTemp:	TEMPORARY						{ $$ = TRUE; }
			| TEMP							{ $$ = TRUE; }
			| LOCAL TEMPORARY				{ $$ = TRUE; }
			| LOCAL TEMP					{ $$ = TRUE; }
			| GLOBAL TEMPORARY				{ $$ = TRUE; }
			| GLOBAL TEMP					{ $$ = TRUE; }
			| /*EMPTY*/						{ $$ = FALSE; }
		;

OptTableElementList:
			TableElementList					{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TableElementList:
			TableElement
				{
					$$ = list_make1($1);
				}
			| TableElementList ',' TableElement
				{
					$$ = lappend($1, $3);
				}
		;

TableElement:
			columnDef							{ $$ = $1; }
			| TableLikeClause					{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
			| column_reference_storage_directive { $$ = $1; }
		;

column_reference_storage_directive:
			COLUMN columnElem ENCODING definition
				{
					ColumnReferenceStorageDirective *n =
						makeNode(ColumnReferenceStorageDirective);

					n->column = (Value *)$2;
					n->encoding = $4;
					$$ = (Node *)n;
				}
			| DEFAULT COLUMN ENCODING definition
				{
					ColumnReferenceStorageDirective *n =
						makeNode(ColumnReferenceStorageDirective);

					n->deflt = true;
					n->encoding = $4;

					$$ = (Node *)n;
				}
		;
				
columnDef:	ColId Typename ColQualList opt_storage_encoding
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typname = $2;
					n->constraints = $3;
					n->is_local = true;
					n->encoding = $4;
					$$ = (Node *)n;
				}
		;

ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					switch (nodeTag($3))
					{
						case T_Constraint:
							{
								Constraint *n = (Constraint *)$3;
								n->name = $2;
							}
							break;
						case T_FkConstraint:
							{
								FkConstraint *n = (FkConstraint *)$3;
								n->constr_name = $2;
							}
							break;
						default:
							break;
					}
					$$ = $3;
				}
			| ColConstraintElem						{ $$ = $1; }
			| ConstraintAttr						{ $$ = $1; }
		;

opt_storage_encoding: ENCODING definition { $$ = $2; }
			| /* nothing */ { $$ = NIL; }
		;

/* DEFAULT NULL is already the default for Postgres.
 * But define it here and carry it forward into the system
 * to make it explicit.
 * - thomas 1998-09-13
 *
 * WITH NULL and NULL are not SQL92-standard syntax elements,
 * so leave them out. Use DEFAULT NULL to explicitly indicate
 * that a column may have that value. WITH NULL leads to
 * shift/reduce conflicts with WITH TIME ZONE anyway.
 * - thomas 1999-01-08
 *
 * DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
 * conflict on NOT (since NOT might start a subsequent NOT NULL constraint,
 * or be part of a_expr NOT LIKE or similar constructs).
 */
ColConstraintElem:
			NOT NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NOTNULL;
					n->name = NULL;
					n->raw_expr = NULL;
					n->cooked_expr = NULL;
					n->keys = NULL;
					n->indexspace = NULL;
					$$ = (Node *)n;
				}
			| NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NULL;
					n->name = NULL;
					n->raw_expr = NULL;
					n->cooked_expr = NULL;
					n->keys = NULL;
					n->indexspace = NULL;
					$$ = (Node *)n;
				}
			| UNIQUE opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->name = NULL;
					n->raw_expr = NULL;
					n->cooked_expr = NULL;
					n->keys = NULL;
					n->options = $2;
					n->indexspace = $3;
					$$ = (Node *)n;
				}
			| PRIMARY KEY opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->name = NULL;
					n->raw_expr = NULL;
					n->cooked_expr = NULL;
					n->keys = NULL;
					n->options = $3;
					n->indexspace = $4;
					$$ = (Node *)n;
				}
			| CHECK '(' a_expr ')'
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->name = NULL;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					n->keys = NULL;
					n->indexspace = NULL;
					$$ = (Node *)n;
				}
			| DEFAULT b_expr
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_DEFAULT;
					n->name = NULL;
					if (exprIsNullConstant($2))
					{
						/* DEFAULT NULL should be reported as empty expr */
						n->raw_expr = NULL;
					}
					else
					{
						n->raw_expr = $2;
					}
					n->cooked_expr = NULL;
					n->keys = NULL;
					n->indexspace = NULL;
					$$ = (Node *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions
				{
					FkConstraint *n = makeNode(FkConstraint);
					n->constr_name		= NULL;
					n->pktable			= $2;
					n->fk_attrs			= NIL;
					n->pk_attrs			= $3;
					n->fk_matchtype		= $4;
					n->fk_upd_action	= (char) ($5 >> 8);
					n->fk_del_action	= (char) ($5 & 0xFF);
					n->deferrable		= FALSE;
					n->initdeferred		= FALSE;
					$$ = (Node *)n;
				}
		;

/*
 * ConstraintAttr represents constraint attributes, which we parse as if
 * they were independent constraint clauses, in order to avoid shift/reduce
 * conflicts (since NOT might start either an independent NOT NULL clause
 * or an attribute).  analyze.c is responsible for attaching the attribute
 * information to the preceding "real" constraint node, and for complaining
 * if attribute clauses appear in the wrong place or wrong combinations.
 *
 * See also ConstraintAttributeSpec, which can be used in places where
 * there is no parsing conflict.
 */
ConstraintAttr:
			DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRABLE;
					$$ = (Node *)n;
				}
			| NOT DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_NOT_DEFERRABLE;
					$$ = (Node *)n;
				}
			| INITIALLY DEFERRED
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRED;
					$$ = (Node *)n;
				}
			| INITIALLY IMMEDIATE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_IMMEDIATE;
					$$ = (Node *)n;
				}
		;


/*
 * SQL99 supports wholesale borrowing of a table definition via the LIKE clause.
 * This seems to be a poor man's inheritance capability, with the resulting
 * tables completely decoupled except for the original commonality in definitions.
 *
 * This is very similar to CREATE TABLE AS except for the INCLUDING DEFAULTS extension
 * which is a part of SQL:2003.
 */
TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					InhRelation *n = makeNode(InhRelation);
					n->relation = $2;
					n->options = $3;
					$$ = (Node *)n;
				}
		;

TableLikeOptionList:
				TableLikeOptionList TableLikeOption	{ $$ = lappend_int($1, $2); }
				| /*EMPTY*/							{ $$ = NIL; }
		;

TableLikeOption:
				INCLUDING DEFAULTS					{ $$ = 	CREATE_TABLE_LIKE_INCLUDING_DEFAULTS; }
				| EXCLUDING DEFAULTS				{ $$ = 	CREATE_TABLE_LIKE_EXCLUDING_DEFAULTS; }
				| INCLUDING CONSTRAINTS				{ $$ = 	CREATE_TABLE_LIKE_INCLUDING_CONSTRAINTS; }
				| EXCLUDING CONSTRAINTS				{ $$ = 	CREATE_TABLE_LIKE_EXCLUDING_CONSTRAINTS; }
				| INCLUDING INDEXES					{ $$ = 	CREATE_TABLE_LIKE_INCLUDING_INDEXES; }
				| EXCLUDING INDEXES					{ $$ = 	CREATE_TABLE_LIKE_EXCLUDING_INDEXES; }
		;


/* ConstraintElem specifies constraint syntax which is not embedded into
 *	a column definition. ColConstraintElem specifies the embedded form.
 * - thomas 1997-12-03
 */
TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					switch (nodeTag($3))
					{
						case T_Constraint:
							{
								Constraint *n = (Constraint *)$3;
								n->name = $2;
							}
							break;
						case T_FkConstraint:
							{
								FkConstraint *n = (FkConstraint *)$3;
								n->constr_name = $2;
							}
							break;
						default:
							break;
					}
					$$ = $3;
				}
			| ConstraintElem						{ $$ = $1; }
		;

ConstraintElem:
			CHECK '(' a_expr ')'
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->name = NULL;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					n->indexspace = NULL;
					$$ = (Node *)n;
				}
			| UNIQUE '(' columnList ')' opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->name = NULL;
					n->raw_expr = NULL;
					n->cooked_expr = NULL;
					n->keys = $3;
					n->options = $5;
					n->indexspace = $6;
					$$ = (Node *)n;
				}
			| PRIMARY KEY '(' columnList ')' opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->name = NULL;
					n->raw_expr = NULL;
					n->cooked_expr = NULL;
					n->keys = $4;
					n->options = $6;
					n->indexspace = $7;
					$$ = (Node *)n;
				}
			| FOREIGN KEY '(' columnList ')' REFERENCES qualified_name
				opt_column_list key_match key_actions ConstraintAttributeSpec
				{
					FkConstraint *n = makeNode(FkConstraint);
					n->constr_name		= NULL;
					n->pktable			= $7;
					n->fk_attrs			= $4;
					n->pk_attrs			= $8;
					n->fk_matchtype		= $9;
					n->fk_upd_action	= (char) ($10 >> 8);
					n->fk_del_action	= (char) ($10 & 0xFF);
					n->deferrable		= ($11 & 1) != 0;
					n->initdeferred		= ($11 & 2) != 0;
					$$ = (Node *)n;
				}
		;

opt_column_list:
			'(' columnList ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
		
opt_inherited_column_list:
			'('
				{
					/* Allowed only when
					 * gp_enable_alter_table_inherit_cols is true 
					 */
					if (!gp_enable_alter_table_inherit_cols)
					{
						yyerror("syntax error");
					}
				}
				columnList ')'						{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

columnList:
			columnElem								{ $$ = list_make1($1); }
			| columnList ',' columnElem				{ $$ = lappend($1, $3); }
		;

columnElem: ColId
				{
					$$ = (Node *) makeString($1);
				}
		;

key_match:  MATCH FULL
			{
				$$ = FKCONSTR_MATCH_FULL;
			}
		| MATCH PARTIAL
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("MATCH PARTIAL not yet implemented"),
						 scanner_errposition(@1)));
				$$ = FKCONSTR_MATCH_PARTIAL;
			}
		| MATCH SIMPLE
			{
				$$ = FKCONSTR_MATCH_UNSPECIFIED;
			}
		| /*EMPTY*/
			{
				$$ = FKCONSTR_MATCH_UNSPECIFIED;
			}
		;

/*
 * We combine the update and delete actions into one value temporarily
 * for simplicity of parsing, and then break them down again in the
 * calling production.  update is in the left 8 bits, delete in the right.
 * Note that NOACTION is the default.
 */
key_actions:
			key_update
				{ $$ = ($1 << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
			| key_delete
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | ($1 & 0xFF); }
			| key_update key_delete
				{ $$ = ($1 << 8) | ($2 & 0xFF); }
			| key_delete key_update
				{ $$ = ($2 << 8) | ($1 & 0xFF); }
			| /*EMPTY*/
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
		;

key_update: ON UPDATE key_action		{ $$ = $3; }
		;

key_delete: ON DELETE_P key_action		{ $$ = $3; }
		;

key_action:
			NO ACTION					{ $$ = FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = FKCONSTR_ACTION_SETDEFAULT; }
		;

OptInherit: INHERITS '(' qualified_name_list ')'	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* WITH (options) is preferred, WITH OIDS and WITHOUT OIDS are legacy forms */
OptWith:
			WITH definition				{ $$ = $2; }
			| WITH OIDS					{ $$ = list_make1(defWithOids(true)); }
			| WITHOUT OIDS				{ $$ = list_make1(defWithOids(false)); }
			| /*EMPTY*/					{ $$ = NIL; }
		;

OnCommitOption:  ON COMMIT DROP				{ $$ = ONCOMMIT_DROP; }
			| ON COMMIT DELETE_P ROWS		{ $$ = ONCOMMIT_DELETE_ROWS; }
			| ON COMMIT PRESERVE ROWS		{ $$ = ONCOMMIT_PRESERVE_ROWS; }
			| /*EMPTY*/						{ $$ = ONCOMMIT_NOOP; }
		;

OptTableSpace:   TABLESPACE name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

OptConsTableSpace:   USING INDEX TABLESPACE name	{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

DistributedBy:   DISTRIBUTED BY  '(' columnList ')'			{ $$ = $4; }
			| DISTRIBUTED RANDOMLY			{ $$ = list_make1(NULL); }
		;

OptDistributedBy:   DistributedBy			{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* START PARTITION RULES */

OptTabPartitionColumnEncList: TabPartitionColumnEncList { $$ = $1; }
			| /*EMPTY*/ { $$ = NULL; }
	;

TabPartitionColumnEncList:
	column_reference_storage_directive { $$ = list_make1($1); }
	| TabPartitionColumnEncList column_reference_storage_directive
				{
					$$ = lappend($1, $2);
				}
	;

OptTabPartitionStorageAttr: WITH definition TABLESPACE name 
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
                    pc->partid = NULL;
                    pc->arg1 = (Node *)$2;
                    pc->arg2 = (Node *)makeString($4);
                    pc->location = @1;
					$$ = (Node *)pc;
                }
			| WITH definition
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
                    pc->partid = NULL;
                    pc->arg1 = (Node *)$2;
                    pc->arg2 = NULL;
                    pc->location = @1;
					$$ = (Node *)pc;
 				}
			| TABLESPACE name 
				{
                    /* re-use alterpartitioncmd struct here... */
					AlterPartitionCmd *pc = makeNode(AlterPartitionCmd);
                    pc->partid = NULL;
                    pc->arg1 = NULL;
                    pc->arg2 = (Node *)makeString($2);
                    pc->location = @1;
					$$ = (Node *)pc;
				}
			| /*EMPTY*/ { $$ = NULL; }
		;

OptTabPartitionsNumber: PARTITIONS IntegerOnly 		
				{
                    /* special rule to disable the use of HASH partitioned
                       tables with nice syntax error.  

                       XXX XXX REMOVE when HASH partitions are in
                       production 
                    */

                    if (!gp_enable_hash_partitioned_tables)
                    {
                        yyerror("syntax error");
                    }
                    
                    $$ = makeAConst($2, @2); 
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

OptTabSubPartitionsNumber: SUBPARTITIONS IntegerOnly 
				{
                    /* special rule to disable the use of HASH partitioned
                       tables with nice syntax error.  

                       XXX XXX REMOVE when HASH partitions are in
                       production 
                    */

                    if (!gp_enable_hash_partitioned_tables)
                    {
                        yyerror("syntax error");
                    }
                    
                    $$ = makeAConst($2, @2); 
                }
			| /*EMPTY*/								{ $$ = NULL; }
		;

OptTabPartitionSpec: '(' TabPartitionElemList ')'
				{
                        PartitionSpec *n = makeNode(PartitionSpec); 
                        n->partElem  = $2;
                        n->subSpec   = NULL;
                        n->location  = @2;
                        $$ = (Node *)n;
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

OptTabSubPartitionSpec: 
            '(' TabSubPartitionElemList ')' 
				{
                        PartitionSpec *n = makeNode(PartitionSpec); 
                        n->partElem  = $2;
                        n->subSpec   = NULL;
                        n->location  = @2;
                        $$ = (Node *)n;
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

TabPartitionElemList:
            TabPartitionElem						{ $$ = list_make1($1); }
			| TabPartitionElemList ',' 
              TabPartitionElem						{ $$ = lappend($1, $3); }
		;
TabSubPartitionElemList:
            TabSubPartitionElem						{ $$ = list_make1($1); }
			| TabSubPartitionElemList ',' 
              TabSubPartitionElem					{ $$ = lappend($1, $3); }
		;

tab_part_val_no_paran: AexprConst { $$ = $1; }
			| CAST '(' tab_part_val AS Typename ')'
				{ 
					$$ = makeTypeCast($3, $5, @4);
				}
			| tab_part_val_no_paran TYPECAST Typename
				{ 
					$$ = makeTypeCast($1, $3, @2); 
				}
			| '-' tab_part_val_no_paran { $$ = doNegate($2, @1); }
		;

tab_part_val: tab_part_val_no_paran { $$ = $1; }
			| '(' tab_part_val_no_paran ')' { $$ = $2; }
			| '(' tab_part_val_no_paran ')' TYPECAST Typename
				{ 
					$$ = makeTypeCast($2, $5, @4); 
				}
		; 
		

TabPartitionBoundarySpecValList:
              tab_part_val				{ $$ = list_make1($1); }
			| TabPartitionBoundarySpecValList ',' 
              tab_part_val				{ $$ = lappend($1, $3); }
		;

/* only optional for START and END in ALTER TABLE...MODIFY PARTITION */
OptTabPartitionBoundarySpecValList:
            TabPartitionBoundarySpecValList			{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

OptTabPartitionRangeInclusive:
			INCLUSIVE			{ $$ = PART_EDGE_INCLUSIVE; }
			| EXCLUSIVE			{ $$ = PART_EDGE_EXCLUSIVE; }
			| /*EMPTY*/			{ $$ = PART_EDGE_UNSPECIFIED; }
		;

TabPartitionBoundarySpecStart:
			START 
            '(' OptTabPartitionBoundarySpecValList ')' 
			OptTabPartitionRangeInclusive
				{
                        PartitionRangeItem *n = makeNode(PartitionRangeItem); 
                        n->partRangeVal  = $3;
                        if (!($5))
                            n->partedge = PART_EDGE_INCLUSIVE;
                        else
                            n->partedge = $5;
                        n->location  = @1;
                        $$ = (Node *)n;
				}
            ;

TabPartitionBoundarySpecEnd:
			END_P 
            '(' OptTabPartitionBoundarySpecValList ')' 
			OptTabPartitionRangeInclusive
				{
                        PartitionRangeItem *n = makeNode(PartitionRangeItem); 
                        n->partRangeVal  = $3;
                        if (!($5))
                            n->partedge = PART_EDGE_EXCLUSIVE;
                        else
                            n->partedge = $5;
                        n->location  = @1;
                        $$ = (Node *)n;
				}
            ;

OptTabPartitionBoundarySpecEvery:
            EVERY '(' TabPartitionBoundarySpecValList ')' 
				{
                        PartitionRangeItem *n = makeNode(PartitionRangeItem); 
                        n->partRangeVal  = $3;
                        n->location  = @1;

                        $$ = (Node *)n;
				}
			| /*EMPTY*/								{ $$ = NULL; }
            ;

OptTabPartitionBoundarySpecEnd:
            TabPartitionBoundarySpecEnd 			{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* VALUES for LIST, start..end for RANGE. */
TabPartitionBoundarySpec:
			part_values_clause
				{
                        PartitionValuesSpec *n = makeNode(PartitionValuesSpec); 

                        n->partValues = $1;
                        n->location  = @1;
                        $$ = (Node *)n;
				}
			| TabPartitionBoundarySpecStart
              OptTabPartitionBoundarySpecEnd
              OptTabPartitionBoundarySpecEvery  
				{
                        PartitionBoundSpec *n = makeNode(PartitionBoundSpec); 
                        n->partStart = $1;
                        n->partEnd   = $2;
                        n->partEvery = $3;
                        n->everyGenList = NIL; 
						n->pWithTnameStr = NULL;
                        n->location  = @1;
                        $$ = (Node *)n;
				}
			| TabPartitionBoundarySpecEnd
              OptTabPartitionBoundarySpecEvery	
				{
                        PartitionBoundSpec *n = makeNode(PartitionBoundSpec); 
                        n->partStart = NULL;
                        n->partEnd   = $1;
                        n->partEvery = $2;
                        n->everyGenList = NIL; 
						n->pWithTnameStr = NULL;
                        n->location  = @1;
                        $$ = (Node *)n;
				}
            ;

OptTabPartitionBoundarySpec:
            TabPartitionBoundarySpec				{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

multi_spec_value_list: '(' part_values_single ')'
				{
					ListCell *lc;
					List *out = NIL;

					foreach(lc, $2)
						out = lappend(out, linitial(lfirst(lc)));

					$$ = list_make1(out);
				}
			| multi_spec_value_list ',' '(' part_values_single ')'
				{
					ListCell *lc;
					List *out = NIL;

					foreach(lc, $4)
						out = lappend(out, linitial(lfirst(lc)));

					$$ = lappend($1, out);
				}
		;

part_values_single: tab_part_val_no_paran
				{
					$$ = list_make1(list_make1($1));
				}
			| part_values_single ',' tab_part_val_no_paran
				{
					$$ = lappend($1, list_make1($3));
				}
		;

part_values_clause:
			VALUES '(' part_values_single ')'
				{
					$$ = $3;
				}
			| VALUES '(' multi_spec_value_list ')'
				{
					$$ = $3;
				}
		;

part_values_or_spec_list: TabPartitionBoundarySpecValList { $$ = $1; }
			| part_values_clause { $$ = $1; }
		;

/* a "Partition Element" closely corresponds to a "Partition Declaration" */
TabPartitionElem: 
            TabPartitionNameDecl 
            OptTabPartitionBoundarySpec	OptTabPartitionStorageAttr
			OptTabPartitionColumnEncList
			OptTabSubPartitionSpec 
				{
                        PartitionElem *n = makeNode(PartitionElem); 
                        n->partName  = $1;
                        n->boundSpec = $2;
                        n->subSpec   = $5;
                        n->location  = @1;
                        n->isDefault = 0;
                        n->storeAttr = $3;
                        n->colencs   = $4;
                        n->AddPartDesc = NULL;
                        $$ = (Node *)n;
				}

/* allow boundary spec for default partition in parser, but complain later */
			| TabPartitionDefaultNameDecl 
              OptTabPartitionBoundarySpec
              OptTabPartitionStorageAttr
			  OptTabPartitionColumnEncList
			  OptTabSubPartitionSpec 
				{
                        PartitionElem *n = makeNode(PartitionElem); 
                        n->partName  = $1;
                        n->boundSpec = $2;
                        n->subSpec   = $5;
                        n->location  = @1;
                        n->isDefault = true;
                        n->storeAttr = $3;
                        n->colencs   = $4;
                        $$ = (Node *)n;
				}
			| TabPartitionBoundarySpec 
              OptTabPartitionStorageAttr
			  OptTabPartitionColumnEncList
			  OptTabSubPartitionSpec 
				{
                        PartitionElem *n = makeNode(PartitionElem); 
                        n->partName  = NULL;
                        n->boundSpec = $1;
                        n->subSpec   = $4;
                        n->location  = @1;
                        n->isDefault = 0;
                        n->storeAttr = $2;
                        n->colencs   = $3;
                        n->AddPartDesc = NULL;
                        $$ = (Node *)n;
				}
			| column_reference_storage_directive
				{
					$$ = (Node *)$1;
				}
            ;

TabSubPartitionElem: 
            TabSubPartitionNameDecl OptTabPartitionBoundarySpec	
			OptTabPartitionStorageAttr
			OptTabPartitionColumnEncList
            OptTabSubPartitionSpec
				{
                        PartitionElem *n = makeNode(PartitionElem); 
                        n->partName  = $1;
                        n->boundSpec = $2;
                        n->subSpec   = $5;
                        n->location  = @1;
                        n->isDefault = 0;
                        n->storeAttr = $3;
                        n->colencs   = $4;
                        n->AddPartDesc = NULL;
                        $$ = (Node *)n;
				}
/* allow boundary spec for default partition in parser, but complain later */
			| TabSubPartitionDefaultNameDecl OptTabPartitionBoundarySpec	
 			  OptTabPartitionStorageAttr
			  OptTabPartitionColumnEncList
              OptTabSubPartitionSpec
				{
                        PartitionElem *n = makeNode(PartitionElem); 
                        n->partName  = $1;
                        n->boundSpec = $2;
                        n->subSpec   = $5;
                        n->location  = @1;
                        n->isDefault = true;
                        n->storeAttr = $3;
                        n->colencs   = $4;
                        n->AddPartDesc = NULL;
                        $$ = (Node *)n;
				}
			| TabPartitionBoundarySpec
              OptTabPartitionStorageAttr
			  OptTabPartitionColumnEncList
 			  OptTabSubPartitionSpec	
				{
                        PartitionElem *n = makeNode(PartitionElem); 
                        n->partName  = NULL;
                        n->boundSpec = $1;
                        n->subSpec   = $4;
                        n->location  = @1;
                        n->isDefault = false;
                        n->colencs   = $3;
                        n->storeAttr = $2;
                        n->AddPartDesc = NULL;
                        $$ = (Node *)n;
				}
			| column_reference_storage_directive
				{
					$$ = (Node *)$1;
				}
            ;

TabPartitionNameDecl: PARTITION PartitionColId
				{
					$$ = (Node *) makeString($2);
				}
		;
TabPartitionDefaultNameDecl: DEFAULT PARTITION PartitionColId
				{
					$$ = (Node *) makeString($3);
				}
		;

TabSubPartitionNameDecl: SUBPARTITION PartitionColId
				{
					$$ = (Node *) makeString($2);
				}
		;

TabSubPartitionDefaultNameDecl: DEFAULT SUBPARTITION PartitionColId
				{
					$$ = (Node *) makeString($3);
				}
		;

partition_hash_keyword: 			HASH
				{
                    /* special rule to disable the use of HASH partitioned
                       tables with nice syntax error.  

                       XXX XXX REMOVE when HASH partitions are in
                       production 
                    */

                    if (!gp_enable_hash_partitioned_tables)
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("PARTITION BY must specify RANGE or LIST"),
                             errOmitLocation(true)));

                    $$ = 1;
                }
		;

partition_coalesce_keyword: 			COALESCE
				{
                    /* special rule to disable the use of HASH partitioned
                       tables with nice syntax error.  

                       XXX XXX REMOVE when HASH partitions are in
                       production 
                    */

                    if (!gp_enable_hash_partitioned_tables)
                    {
                        yyerror("syntax error");
                    }

                    $$ = 1;
                }
		;

TabPartitionByType:
			RANGE 				{ $$ = PARTTYP_RANGE; }
			| partition_hash_keyword { $$ = PARTTYP_HASH; }
			| LIST				{ $$ = PARTTYP_LIST; }
			| /*EMPTY*/
				{
					$$ = PARTTYP_RANGE; 

                    if (!gp_enable_hash_partitioned_tables)
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("PARTITION BY must specify RANGE or LIST")));
                    else
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("PARTITION BY must specify RANGE, HASH, or LIST")));

                    
				}
		;

OptTabPartitionBy:
			PARTITION BY 
            TabPartitionByType '(' columnList ')' 
            OptTabPartitionsNumber 
			opt_list_subparts
            OptTabPartitionSpec						
				{
					PartitionBy *n = makeNode(PartitionBy); 
						
					n->partType = $3;
					n->keys     = $5; 
					n->partNum  = $7;
					n->subPart  = $8;
					if (PointerIsValid(n->subPart) &&
						!IsA(n->subPart, PartitionBy))
						yyerror("syntax error");

					n->partSpec = $9;
					n->partDepth = 0;
					n->partQuiet = PART_VERBO_NODISTRO;
					n->location  = @3;
					n->partDefault = NULL;
					$$ = (Node *)n;
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

TabSubPartitionTemplate:
			SUBPARTITION TEMPLATE 
            '(' TabSubPartitionElemList ')' 
				{
					PartitionSpec *n = makeNode(PartitionSpec); 
					n->partElem  = $4;
					n->subSpec   = NULL;
					n->istemplate  = true;
					n->location  = @3;
					$$ = (Node *)n;

					/* a little (temporary?) syntax check on templates */
					if (n->partElem)
					{
						List *elems;
						ListCell *lc;
						Assert(IsA(n->partElem, List));

						elems = (List *)n->partElem;
						foreach(lc, elems)
						{
							PartitionElem *e = lfirst(lc);

							if (!IsA(e, PartitionElem)) continue;

							if (e->subSpec)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("template cannot contain "
												"specification for child "
												"partition"),
										 errOmitLocation(true)));
						}

					}
				}
		;

opt_list_subparts: list_subparts { $$ = $1; }
			| /*EMPTY*/ { $$ = NULL; }
		;

opt_comma: ',' { $$ = true; }
			| /*EMPTY*/ { $$ = false; }
		;

list_subparts: TabSubPartitionBy { $$ = $1; }
			| list_subparts opt_comma TabSubPartitionBy
				{
					PartitionBy *pby = (PartitionBy *)$1;
					$$ = $1;

					while (pby->subPart)
						pby = (PartitionBy *)pby->subPart;

					if (IsA($3, PartitionSpec))
					{
						Assert(((PartitionSpec *)$3)->istemplate);
						Assert(IsA(pby, PartitionBy));

						/* find deepesh subpart and add it there */
						if (((PartitionBy *)pby)->partSpec != NULL)
							yyerror("syntax error");

						((PartitionBy *)pby)->partSpec = $3;
					}
					else
						((PartitionBy *)pby)->subPart = $3;
				}
		;

TabSubPartitionBy:
			SUBPARTITION BY 
            TabPartitionByType '(' columnList ')' 
            OptTabSubPartitionsNumber 
				{
                        PartitionBy *n = makeNode(PartitionBy); 
                        n->partType = $3;
                        n->keys     = $5; 
                        n->partNum  = $7;
                        n->subPart  = NULL;
                        n->partSpec = NULL;
                        n->partDepth = 0;
						n->partQuiet = PART_VERBO_NODISTRO;
                        n->location  = @3;
                        n->partDefault = NULL;
                        $$ = (Node *)n;
				}
			| TabSubPartitionTemplate
				{
					$$ = $1;
				}
		;
/* END PARTITION RULES */

/*
 * Note: CREATE TABLE ... AS SELECT ... is just another spelling for
 * SELECT ... INTO.
 */
	
CreateAsStmt:
		CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data OptDistributedBy OptTabPartitionBy
				{
					/*
					 * When the SelectStmt is a set-operation tree, we must
					 * stuff the INTO information into the leftmost component
					 * Select, because that's where analyze.c will expect
					 * to find it.	Similarly, the output column names must
					 * be attached to that Select's target list.
					 */
					SelectStmt *n = findLeftmostSelect((SelectStmt *) $6);
					if (n->intoClause != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE AS cannot specify INTO"),
								 scanner_errposition(exprLocation((Node *) n->intoClause))));
					$4->rel->istemp = $2;
					n->intoClause = $4;
					/* Implement WITH NO DATA by forcing top-level LIMIT 0 */
					if (!$7)
						((SelectStmt *) $6)->limitCount = makeIntConst(0, -1);
					n->distributedBy = $8;
					
					if ($9)
						ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("Cannot create a partitioned table using CREATE TABLE AS SELECT"),
                                 errhint("Use CREATE TABLE...LIKE (followed by INSERT...SELECT) instead"),
                                 errOmitLocation(true)));
					
					$$ = $6;
				}
		;

create_as_target:
			qualified_name OptCreateAs OptWith OnCommitOption OptTableSpace
				{
					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = $4;
					$$->tableSpaceName = $5;
				}
		;

OptCreateAs:
			'(' CreateAsList ')'					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

CreateAsList:
			CreateAsElement							{ $$ = list_make1($1); }
			| CreateAsList ',' CreateAsElement		{ $$ = lappend($1, $3); }
		;

CreateAsElement:
			ColId
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typname = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->constraints = NIL;
					$$ = (Node *)n;
				}
		;

opt_with_data:
			WITH DATA_P								{ $$ = TRUE; }
			| WITH NO DATA_P						{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = TRUE; }
		;

/*****************************************************************************
 *
 * 		QUERY:
 *             CREATE FOREIGN TABLE relname SERVER srvname [OPTIONS]
 * 		
 *		NOTE: we use OptExtTableElementList, to enforce no constraints.
 *****************************************************************************/

CreateForeignStmt: CREATE FOREIGN OptTemp TABLE qualified_name '(' OptExtTableElementList ')' 
				   SERVER name create_generic_options
				{
					CreateForeignStmt *n = makeNode(CreateForeignStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }		
					n->relation = $5;
					$5->istemp = $3;
					n->tableElts = $7;
					n->srvname = $10;
					n->options = $11;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE EXTERNAL [WEB] TABLE relname
 *
 *****************************************************************************/
	
CreateExternalStmt:	CREATE OptWritable EXTERNAL OptWeb OptTemp TABLE qualified_name '(' OptExtTableElementList ')' 
					ExtTypedesc FORMAT Sconst format_opt ext_opt_encoding_list OptSingleRowErrorHandling OptDistributedBy
						{
							CreateExternalStmt *n = makeNode(CreateExternalStmt);
							n->iswritable = $2;
							n->isweb = $4;
							$7->istemp = $5;
							n->relation = $7;
							n->tableElts = $9;
							n->exttypedesc = $11;
							n->format = $13;
							n->formatOpts = $14;
							n->encoding = $15;
							n->sreh = $16;
							n->distributedBy = $17;
							n->policy = 0;
							
							/* various syntax checks for EXECUTE external table */
							if(((ExtTableTypeDesc *) n->exttypedesc)->exttabletype == EXTTBL_TYPE_EXECUTE)
							{
								ExtTableTypeDesc *extdesc = (ExtTableTypeDesc *) n->exttypedesc;
								
								if(!n->isweb)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
										 	 errmsg("EXECUTE may not be used with a regular external table"),
										 	 errhint("Use CREATE EXTERNAL WEB TABLE instead"),
										 	 errOmitLocation(true)));							
								
								/* if no ON clause specified, default to "ON ALL" */
								if(extdesc->on_clause == NIL)
								{									
									extdesc->on_clause = lappend(extdesc->on_clause, 
										   				   		 makeDefElem("all", (Node *)makeInteger(TRUE)));
								}
							}

							if(n->sreh && n->iswritable)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("Single row error handling may not be used with a writable external table")));							
							
							$$ = (Node *)n;							
						}
						;

OptWritable:	WRITABLE				{ $$ = TRUE; }
				| READABLE				{ $$ = FALSE; }
				| /*EMPTY*/				{ $$ = FALSE; }
				;

OptWeb:		WEB						{ $$ = TRUE; }
			| /*EMPTY*/				{ $$ = FALSE; }
			;

ExtTypedesc:
			LOCATION '(' cdb_string_list ')'		
			{
				ExtTableTypeDesc *n = makeNode(ExtTableTypeDesc);
				n->exttabletype = EXTTBL_TYPE_LOCATION;
				n->location_list = $3; 
				n->on_clause = NIL;
				n->command_string = NULL;
				$$ = (Node *)n;
	
			}
			| EXECUTE Sconst ext_on_clause_list
			{
				ExtTableTypeDesc *n = makeNode(ExtTableTypeDesc);
				n->exttabletype = EXTTBL_TYPE_EXECUTE;
				n->location_list = NIL; 
				n->command_string = $2;
				n->on_clause = $3; /* default will get set later if needed */
						
				$$ = (Node *)n;
			}
			;

ext_on_clause_list:
			ext_on_clause_list ext_on_clause_item		{ $$ = lappend($1, $2); }
			| /*EMPTY*/									{ $$ = NIL; }
			;
	
ext_on_clause_item:
			ON ALL	
			{
				$$ = makeDefElem("all", (Node *)makeInteger(TRUE));
			}
			| ON HOST Sconst
			{
				$$ = makeDefElem("hostname", (Node *)makeString($3));
			}
			| ON HOST
			{
				$$ = makeDefElem("eachhost", (Node *)makeInteger(TRUE));
			}
			| ON MASTER
			{
				$$ = makeDefElem("master", (Node *)makeInteger(TRUE));
			}
			| ON SEGMENT Iconst
			{
				$$ = makeDefElem("segment", (Node *)makeInteger($3));
			}
			| ON Iconst
			{
				$$ = makeDefElem("random", (Node *)makeInteger($2));
			}
			;

format_opt: 
			  '(' format_opt_list ')'			{ $$ = $2; }
			| '(' format_def_list ')'			{ $$ = $2; }
			| '(' ')'							{ $$ = NIL; }
			| /*EMPTY*/							{ $$ = NIL; }
			;

format_opt_list:
			format_opt_item		
			{ 
				$$ = list_make1($1);
			}
			| format_opt_list format_opt_item		
			{ 
				$$ = lappend($1, $2); 
			}
			;

format_def_list:
			format_def_item		
			{ 
				$$ = list_make1($1);
			} 
			| format_def_list ',' format_def_item
			{
				$$ = lappend($1, $3);
			}

format_def_item:
    		ColLabel '=' def_arg
			{
				$$ = makeDefElem($1, $3);
			}
			| ColLabel '=' '(' columnList ')'
			{
				$$ = makeDefElem($1, (Node *) $4);
			}


format_opt_item:
			DELIMITER opt_as Sconst
			{
				$$ = makeDefElem("delimiter", (Node *)makeString($3));
			}
			| NULL_P opt_as Sconst
			{
				$$ = makeDefElem("null", (Node *)makeString($3));
			}
			| CSV
			{
				$$ = makeDefElem("csv", (Node *)makeInteger(TRUE));
			}
			| HEADER_P
			{
				$$ = makeDefElem("header", (Node *)makeInteger(TRUE));
			}
			| QUOTE opt_as Sconst
			{
				$$ = makeDefElem("quote", (Node *)makeString($3));
			}
			| ESCAPE opt_as Sconst
			{
				$$ = makeDefElem("escape", (Node *)makeString($3));
			}
			| FORCE NOT NULL_P columnList
			{
				$$ = makeDefElem("force_notnull", (Node *)$4);
			}
			| FORCE QUOTE columnList
			{
				$$ = makeDefElem("force_quote", (Node *)$3);
			}
			| FILL MISSING FIELDS
			{
				$$ = makeDefElem("fill_missing_fields", (Node *)makeInteger(TRUE));
			}
			| NEWLINE opt_as Sconst
			{
				$$ = makeDefElem("newline", (Node *)makeString($3));
			}
			;

OptExtTableElementList:
			ExtTableElementList				{ $$ = $1; }
			| /*EMPTY*/						{ $$ = NIL; }
			;

ExtTableElementList:
			ExtTableElement
			{
				$$ = list_make1($1);
			}
			| ExtTableElementList ',' ExtTableElement
			{
				$$ = lappend($1, $3);
			}
			;

ExtTableElement:
			ExtcolumnDef					{ $$ = $1; }
			| TableLikeClause				{ $$ = $1; }
			;

/* column def for ext table - doesn't have room for constraints */
ExtcolumnDef:	ColId Typename
		{
			ColumnDef *n = makeNode(ColumnDef);
			n->colname = $1;
			n->typname = $2;
			n->is_local = true;
			n->is_not_null = false;
			n->constraints = NIL;
			$$ = (Node *)n;
		}
		;
	
/*
 * Single row error handling SQL
 */
OptSingleRowErrorHandling:
		OptErrorTableName OptSrehKeep SEGMENT REJECT_P LIMIT Iconst OptSrehLimitType
		{
			SingleRowErrorDesc *n = makeNode(SingleRowErrorDesc);
			n->errtable = $1;
			n->is_keep = $2;
			n->rejectlimit = $6;
			n->is_limit_in_rows = $7; /* true for ROWS false for PERCENT */
			
			/* PERCENT value check */
			if(!n->is_limit_in_rows && (n->rejectlimit < 1 || n->rejectlimit > 100))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid PERCENT value. Should be (1 - 100)")));
			
			/* ROW values check */
			if(n->is_limit_in_rows && n->rejectlimit < 2)
			   ereport(ERROR,
					   (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid (ROWS) reject limit. Should be 2 or larger")));
			
			$$ = (Node *)n;
		}
		| /*EMPTY*/		{ $$ = NULL; }
		;
	
OptErrorTableName:
		LOG_P ERRORS INTO qualified_name 	{ $$ = $4; }
		| /*EMPTY*/							{ $$ = NULL; }
		;
	
OptSrehLimitType:		
		ROWS					{ $$ = TRUE; }
		| PERCENT				{ $$ = FALSE; }
		| /* default is ROWS */	{ $$ = TRUE; }
		;

OptSrehKeep:
		KEEP				{ $$ = TRUE; }
		| /*EMPTY*/			{ $$ = FALSE; }
		;

/*
 * ENCODING. (we cheat a little and use a list, even though it's 1 item max).
 */
ext_opt_encoding_list:
		ext_opt_encoding_list ext_opt_encoding_item		{ $$ = lappend($1, $2); }
		| /*EMPTY*/										{ $$ = NIL; }
		;
	
ext_opt_encoding_item:
		ENCODING opt_equal Sconst
		{
			$$ = makeDefElem("encoding", (Node *)makeString($3));
		}
		| ENCODING opt_equal Iconst
		{
			$$ = makeDefElem("encoding", (Node *)makeInteger($3));
		}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/

CreateSeqStmt:
			CREATE OptTemp SEQUENCE qualified_name OptSeqList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$4->istemp = $2;
					n->sequence = $4;
					n->options = $5;
					n->relOid = 0;
					$$ = (Node *)n;
				}
		;

AlterSeqStmt:
			ALTER SEQUENCE qualified_name OptSeqList
				{
					AlterSeqStmt *n = makeNode(AlterSeqStmt);
					n->sequence = $3;
					n->options = $4;
					$$ = (Node *)n;
				}
		;

OptSeqList: OptSeqList OptSeqElem					{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

OptSeqElem: CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (Node *)$2);
				}
			| CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(TRUE));
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(FALSE));
				}
			| INCREMENT opt_by NumericOnly
				{
					$$ = makeDefElem("increment", (Node *)$3);
				}
			| MAXVALUE NumericOnly
				{
					$$ = makeDefElem("maxvalue", (Node *)$2);
				}
			| MINVALUE NumericOnly
				{
					$$ = makeDefElem("minvalue", (Node *)$2);
				}
			| NO MAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (Node *)$3);
				}
			| START opt_with NumericOnly
				{
					$$ = makeDefElem("start", (Node *)$3);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (Node *)$3);
				}
		;

opt_by:		BY				{}
			| /*EMPTY*/		{}
	  ;

NumericOnly:
			FloatOnly								{ $$ = $1; }
			| IntegerOnly							{ $$ = $1; }
		;

FloatOnly:	FCONST									{ $$ = makeFloat($1); }
			| '-' FCONST
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
		;

IntegerOnly: SignedIconst							{ $$ = makeInteger($1); };


/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE PROCEDURAL LANGUAGE ...
 *				DROP PROCEDURAL LANGUAGE ...
 *
 *****************************************************************************/

CreatePLangStmt:
			CREATE opt_trusted opt_procedural LANGUAGE ColId_or_Sconst
			{
				CreatePLangStmt *n = makeNode(CreatePLangStmt);
				n->plname = $5;
				/* parameters are all to be supplied by system */
				n->plhandler = NIL;
				n->plvalidator = NIL;
				n->pltrusted = false;
				$$ = (Node *)n;
			}
			| CREATE opt_trusted opt_procedural LANGUAGE ColId_or_Sconst
			  HANDLER handler_name opt_validator opt_lancompiler
			{
				CreatePLangStmt *n = makeNode(CreatePLangStmt);
				n->plname = $5;
				n->plhandler = $7;
				n->plvalidator = $8;
				n->pltrusted = $2;
				/* LANCOMPILER is now ignored entirely */
				$$ = (Node *)n;
			}
		;

opt_trusted:
			TRUSTED									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* This ought to be just func_name, but that causes reduce/reduce conflicts
 * (CREATE LANGUAGE is the only place where func_name isn't followed by '(').
 * Work around by using simple names, instead.
 */
handler_name:
			name						{ $$ = list_make1(makeString($1)); }
			| name attrs				{ $$ = lcons(makeString($1), $2); }
		;

validator_clause:
			VALIDATOR handler_name					{ $$ = $2; }
			| NO VALIDATOR							{ $$ = NIL; }
		;

opt_validator:
			validator_clause						{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_lancompiler:
			LANCOMPILER Sconst						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

DropPLangStmt:
			DROP opt_procedural LANGUAGE ColId_or_Sconst opt_drop_behavior
				{
					DropPLangStmt *n = makeNode(DropPLangStmt);
					n->plname = $4;
					n->behavior = $5;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| DROP opt_procedural LANGUAGE IF_P EXISTS ColId_or_Sconst opt_drop_behavior
				{
					DropPLangStmt *n = makeNode(DropPLangStmt);
					n->plname = $6;
					n->behavior = $7;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

opt_procedural:
			PROCEDURAL								{}
			| /*EMPTY*/								{}
		;

/*****************************************************************************
 *
 * 		QUERY:
 *             CREATE FILESPACE filespace ( ... )
 *
 *****************************************************************************/

CreateFileSpaceStmt: 
			CREATE FILESPACE name OptOwner OptStorage '(' Sconst ')' opt_definition
				{
					CreateFileSpaceStmt *n = makeNode(CreateFileSpaceStmt);
					n->filespacename = $3;
					n->owner = $4;
					n->fsysname = $5;
					n->location = $7;
					n->options = $9;
					$$ = (Node *) n;
				}
		;

OptOwner: 
			OWNER name			{ $$ = $2; }
			| /*EMPTY*/			{ $$ = NULL; }
		;

OptStorage:
			ON name				{ $$ = $2; }
 			| /*EMPTY*/			{ $$ = "local"; }

/*****************************************************************************
 *
 * 		QUERY:
 *             CREATE TABLESPACE tablespace FILESPACE filespace
 *
 *****************************************************************************/

CreateTableSpaceStmt: CREATE TABLESPACE name OptOwner FILESPACE name
				{
					CreateTableSpaceStmt *n = makeNode(CreateTableSpaceStmt);
					n->tablespacename = $3;
					n->owner = $4;
					n->filespacename = $6;
					n->tsoid = 0;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * 		QUERY:
 *             CREATE FOREIGN DATA WRAPPER name [ VALIDATOR name ]
 *
 *****************************************************************************/

CreateFdwStmt: CREATE FOREIGN DATA_P WRAPPER name opt_validator create_generic_options
				{
					CreateFdwStmt *n = makeNode(CreateFdwStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->fdwname = $5;
					n->validator = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * 		QUERY :
 *				DROP FOREIGN DATA WRAPPER name
 *
 ****************************************************************************/

DropFdwStmt: DROP FOREIGN DATA_P WRAPPER name opt_drop_behavior
				{
					DropFdwStmt *n = makeNode(DropFdwStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->fdwname = $5;
					n->missing_ok = false;
					n->behavior = $6;
					$$ = (Node *) n;
				}
				|  DROP FOREIGN DATA_P WRAPPER IF_P EXISTS name opt_drop_behavior
                {
					DropFdwStmt *n = makeNode(DropFdwStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->fdwname = $7;
					n->missing_ok = true;
					n->behavior = $8;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * 		QUERY :
 *				ALTER FOREIGN DATA WRAPPER name
 *
 ****************************************************************************/

AlterFdwStmt: ALTER FOREIGN DATA_P WRAPPER name validator_clause alter_generic_options
				{
					AlterFdwStmt *n = makeNode(AlterFdwStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }					
					n->fdwname = $5;
					n->validator = $6;
					n->change_validator = true;
					n->options = $7;
					$$ = (Node *) n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name validator_clause
				{
					AlterFdwStmt *n = makeNode(AlterFdwStmt);

					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->fdwname = $5;
					n->validator = $6;
					n->change_validator = true;
					$$ = (Node *) n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name alter_generic_options
				{
					AlterFdwStmt *n = makeNode(AlterFdwStmt);

					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->fdwname = $5;
					n->options = $6;
					$$ = (Node *) n;
				}
		;

/* Options definition for CREATE FDW, SERVER, USER MAPPING and FOREIGN TABLE */
create_generic_options:
			OPTIONS '(' generic_option_list ')'			{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NIL; }
		;

generic_option_list:
			generic_option_elem
				{
					$$ = list_make1($1);
				}
			| generic_option_list ',' generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

/* Options definition for ALTER FDW, SERVER, USER MAPPING and FOREIGN TABLE */
alter_generic_options:
			OPTIONS	'(' alter_generic_option_list ')'		{ $$ = $3; }
		;

alter_generic_option_list:
			alter_generic_option_elem
				{
					$$ = list_make1($1);
				}
			| alter_generic_option_list ',' alter_generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

alter_generic_option_elem:
			generic_option_elem
				{
					$$ = $1;
				}
			| SET generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_SET;
				}
			| ADD_P generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_ADD;
				}
			| DROP generic_option_name
				{
					$$ = makeDefElemExtended($2, NULL, DEFELEM_DROP);
				}
		;

generic_option_elem:
			generic_option_name generic_option_arg
				{
					$$ = makeDefElem($1, $2);
				}
		;

generic_option_name:
				ColLabel			{ $$ = $1; }
		;

/* We could use def_arg here, but the spec only requires string literals */
generic_option_arg:
				Sconst				{ $$ = (Node *) makeString($1); }
		;

/*****************************************************************************
 *
 * 		QUERY:
 *             CREATE SERVER name [TYPE] [VERSION] [OPTIONS]
 *
 *****************************************************************************/

CreateForeignServerStmt: CREATE SERVER name opt_type opt_foreign_server_version
						 FOREIGN DATA_P WRAPPER name create_generic_options
				{
					CreateForeignServerStmt *n = makeNode(CreateForeignServerStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->servername = $3;
					n->servertype = $4;
					n->version = $5;
					n->fdwname = $9;
					n->options = $10;
					$$ = (Node *) n;
				}
		;

opt_type:
			TYPE_P Sconst			{ $$ = $2; }
			| /*EMPTY*/				{ $$ = NULL; }
		;


foreign_server_version:
			VERSION_P Sconst		{ $$ = $2; }
		|	VERSION_P NULL_P		{ $$ = NULL; }
		;

opt_foreign_server_version:
			foreign_server_version 	{ $$ = $1; }
			| /*EMPTY*/				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 * 		QUERY :
 *				DROP SERVER name
 *
 ****************************************************************************/

DropForeignServerStmt: DROP SERVER name opt_drop_behavior
				{
					DropForeignServerStmt *n = makeNode(DropForeignServerStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->servername = $3;
					n->missing_ok = false;
					n->behavior = $4;
					$$ = (Node *) n;
				}
				|  DROP SERVER IF_P EXISTS name opt_drop_behavior
                {
					DropForeignServerStmt *n = makeNode(DropForeignServerStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->servername = $5;
					n->missing_ok = true;
					n->behavior = $6;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * 		QUERY :
 *				ALTER SERVER name [VERSION] [OPTIONS]
 *
 ****************************************************************************/

AlterForeignServerStmt: ALTER SERVER name foreign_server_version alter_generic_options
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->servername = $3;
					n->version = $4;
					n->options = $5;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER SERVER name foreign_server_version
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->servername = $3;
					n->version = $4;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER SERVER name alter_generic_options
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					
					if (!gp_foreign_data_access)
                    {
                        yyerror("syntax error");
                    }
					n->servername = $3;
					n->options = $4;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * 		QUERY:
 *             CREATE USER MAPPING FOR auth_ident SERVER name [OPTIONS]
 *
 *****************************************************************************/

CreateUserMappingStmt: CREATE USER MAPPING FOR auth_ident SERVER name create_generic_options
				{
					CreateUserMappingStmt *n = makeNode(CreateUserMappingStmt);
					n->username = $5;
					n->servername = $7;
					n->options = $8;
					$$ = (Node *) n;
				}
		;

/* User mapping authorization identifier */
auth_ident:
			CURRENT_USER 	{ $$ = "current_user"; }
		|	USER			{ $$ = "current_user"; }
		|	RoleId 			{ $$ = (strcmp($1, "public") == 0) ? NULL : $1; }
		;

/*****************************************************************************
 *
 * 		QUERY :
 *				DROP USER MAPPING FOR auth_ident SERVER name
 *
 ****************************************************************************/

DropUserMappingStmt: DROP USER MAPPING FOR auth_ident SERVER name
				{
					DropUserMappingStmt *n = makeNode(DropUserMappingStmt);
					n->username = $5;
					n->servername = $7;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
				|  DROP USER MAPPING IF_P EXISTS FOR auth_ident SERVER name
                {
					DropUserMappingStmt *n = makeNode(DropUserMappingStmt);
					n->username = $7;
					n->servername = $9;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * 		QUERY :
 *				ALTER USER MAPPING FOR auth_ident SERVER name OPTIONS
 *
 ****************************************************************************/

AlterUserMappingStmt: ALTER USER MAPPING FOR auth_ident SERVER name alter_generic_options
				{
					AlterUserMappingStmt *n = makeNode(AlterUserMappingStmt);
					n->username = $5;
					n->servername = $7;
					n->options = $8;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE TRIGGER ...
 *				DROP TRIGGER ...
 *
 *****************************************************************************/

CreateTrigStmt:
			CREATE TRIGGER name TriggerActionTime TriggerEvents ON
			qualified_name TriggerForSpec EXECUTE PROCEDURE
			func_name '(' TriggerFuncArgs ')'
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $3;
					n->relation = $7;
					n->funcname = $11;
					n->args = $13;
					n->before = $4;
					n->row = $8;
					memcpy(n->actions, $5, 4);
					n->isconstraint  = FALSE;
					n->deferrable	 = FALSE;
					n->initdeferred  = FALSE;
					n->constrrel = NULL;
					$$ = (Node *)n;
				}
			| CREATE CONSTRAINT TRIGGER name AFTER TriggerEvents ON
			qualified_name OptConstrFromTable
			ConstraintAttributeSpec
			FOR EACH ROW EXECUTE PROCEDURE
			func_name '(' TriggerFuncArgs ')'
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $4;
					n->relation = $8;
					n->funcname = $16;
					n->args = $18;
					n->before = FALSE;
					n->row = TRUE;
					memcpy(n->actions, $6, 4);
					n->isconstraint  = TRUE;
					n->deferrable = ($10 & 1) != 0;
					n->initdeferred = ($10 & 2) != 0;

					n->constrrel = $9;
					$$ = (Node *)n;
				}
		;

TriggerActionTime:
			BEFORE									{ $$ = TRUE; }
			| AFTER									{ $$ = FALSE; }
		;

TriggerEvents:
			TriggerOneEvent
				{
					char *e = palloc(4);
					e[0] = $1; e[1] = '\0';
					$$ = e;
				}
			| TriggerOneEvent OR TriggerOneEvent
				{
					char *e = palloc(4);
					e[0] = $1; e[1] = $3; e[2] = '\0';
					$$ = e;
				}
			| TriggerOneEvent OR TriggerOneEvent OR TriggerOneEvent
				{
					char *e = palloc(4);
					e[0] = $1; e[1] = $3; e[2] = $5; e[3] = '\0';
					$$ = e;
				}
		;

TriggerOneEvent:
			INSERT									{ $$ = 'i'; }
			| DELETE_P								{ $$ = 'd'; }
			| UPDATE								{ $$ = 'u'; }
		;

TriggerForSpec:
			FOR TriggerForOpt TriggerForType
				{
					$$ = $3;
				}
			| /*EMPTY*/
				{
					/*
					 * If ROW/STATEMENT not specified, default to
					 * STATEMENT, per SQL
					 */
					$$ = FALSE;
				}
		;

TriggerForOpt:
			EACH									{}
			| /*EMPTY*/								{}
		;

TriggerForType:
			ROW										{ $$ = TRUE; }
			| STATEMENT								{ $$ = FALSE; }
		;

TriggerFuncArgs:
			TriggerFuncArg							{ $$ = list_make1($1); }
			| TriggerFuncArgs ',' TriggerFuncArg	{ $$ = lappend($1, $3); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

TriggerFuncArg:
			ICONST
				{
					char buf[64];
					snprintf(buf, sizeof(buf), "%d", $1);
					$$ = makeString(pstrdup(buf));
				}
			| FCONST								{ $$ = makeString($1); }
			| Sconst								{ $$ = makeString($1); }
			| BCONST								{ $$ = makeString($1); }
			| XCONST								{ $$ = makeString($1); }
			| ColId									{ $$ = makeString($1); }
		;

OptConstrFromTable:
			FROM qualified_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ConstraintAttributeSpec:
			ConstraintDeferrabilitySpec
				{ $$ = $1; }
			| ConstraintDeferrabilitySpec ConstraintTimeSpec
				{
					if ($1 == 0 && $2 != 0)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
								 scanner_errposition(@1)));
					$$ = $1 | $2;
				}
			| ConstraintTimeSpec
				{
					if ($1 != 0)
						$$ = 3;
					else
						$$ = 0;
				}
			| ConstraintTimeSpec ConstraintDeferrabilitySpec
				{
					if ($2 == 0 && $1 != 0)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
								 scanner_errposition(@1)));
					$$ = $1 | $2;
				}
			| /*EMPTY*/
				{ $$ = 0; }
		;

ConstraintDeferrabilitySpec:
			NOT DEFERRABLE							{ $$ = 0; }
			| DEFERRABLE							{ $$ = 1; }
		;

ConstraintTimeSpec:
			INITIALLY IMMEDIATE						{ $$ = 0; }
			| INITIALLY DEFERRED					{ $$ = 2; }
		;


DropTrigStmt:
			DROP TRIGGER name ON qualified_name opt_drop_behavior
				{
					DropPropertyStmt *n = makeNode(DropPropertyStmt);
					n->relation = $5;
					n->property = $3;
					n->behavior = $6;
					n->removeType = OBJECT_TRIGGER;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
			| DROP TRIGGER IF_P EXISTS name ON qualified_name opt_drop_behavior
				{
					DropPropertyStmt *n = makeNode(DropPropertyStmt);
					n->relation = $7;
					n->property = $5;
					n->behavior = $8;
					n->removeType = OBJECT_TRIGGER;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE ASSERTION ...
 *				DROP ASSERTION ...
 *
 *****************************************************************************/

CreateAssertStmt:
			CREATE ASSERTION name CHECK '(' a_expr ')'
			ConstraintAttributeSpec
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $3;
					n->args = list_make1($6);
					n->isconstraint  = TRUE;
					n->deferrable = ($8 & 1) != 0;
					n->initdeferred = ($8 & 2) != 0;

					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CREATE ASSERTION is not yet implemented")));

					$$ = (Node *)n;
				}
		;

DropAssertStmt:
			DROP ASSERTION name opt_drop_behavior
				{
					DropPropertyStmt *n = makeNode(DropPropertyStmt);
					n->relation = NULL;
					n->property = $3;
					n->behavior = $4;
					n->removeType = OBJECT_TRIGGER; /* XXX */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DROP ASSERTION is not yet implemented")));
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				define (aggregate,operator,type)
 *
 *****************************************************************************/

DefineStmt:
			CREATE opt_ordered AGGREGATE func_name aggr_args definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = false;
					n->defnames = $4;
					n->args = $5;
					n->definition = $6;
					n->ordered = $2;
					$$ = (Node *)n;
				}
			| CREATE opt_ordered AGGREGATE func_name old_aggr_definition
				{
					/* old-style (pre-8.2) syntax for CREATE AGGREGATE */
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = true;
					n->defnames = $4;
					n->args = NIL;
					n->definition = $5;
					n->newOid = 0;
					n->ordered = $2;
					$$ = (Node *)n;
				}
			| CREATE OPERATOR any_operator definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_OPERATOR;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					n->newOid = 0;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					n->newOid = 0;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name 
				{
					/* Shell type (identified by lack of definition) */
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = NIL;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name AS '(' TableFuncElementList ')'
				{
					CompositeTypeStmt *n = makeNode(CompositeTypeStmt);
					RangeVar *r = makeNode(RangeVar);
					r->location = @3;

					/* can't use qualified_name, sigh */
					switch (list_length($3))
					{
						case 1:
							r->catalogname = NULL;
							r->schemaname = NULL;
							r->relname = strVal(linitial($3));
							break;
						case 2:
							r->catalogname = NULL;
							r->schemaname = strVal(linitial($3));
							r->relname = strVal(lsecond($3));
							break;
						case 3:
							r->catalogname = strVal(linitial($3));
							r->schemaname = strVal(lsecond($3));
							r->relname = strVal(lthird($3));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString($3)),
											scanner_errposition(@3)));
							break;
					}
					n->typevar = r;
					n->coldeflist = $6;
					n->relOid = 0;
					n->comptypeOid = 0;
					$$ = (Node *)n;
				}
			| CREATE opt_trusted PROTOCOL name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_EXTPROTOCOL;
					n->oldstyle = false;
					n->trusted = $2;
					n->defnames = list_make1(makeString($4));
					n->args = NIL;
					n->newOid = 0;
					n->definition = $5;
					n->ordered = false;
					$$ = (Node *)n;
				}
			| CREATE opt_trusted FILESYSTEM any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_FILESYSTEM;
					n->oldstyle = false;
					n->trusted = $2;
					n->defnames = $4;
					n->args = NIL;
					n->newOid = 0;
					n->definition = $5;
					n->ordered = false;
					$$ = (Node *)n;
				}
		;

opt_ordered:	ORDERED	{ $$ = TRUE; }
			| /*EMPTY*/	{ $$ = FALSE; }
		;

definition: '(' def_list ')'						{ $$ = $2; }
		;

def_list:  	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;

def_elem:  ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *)$3);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL);
				}
		;

/* Note: any simple identifier will be returned as a type name! */
def_arg:	func_type						{ $$ = (Node *)$1; }
			/* MPP-6685: allow unquoted ROW keyword as "orientation" option */
			| ROW							{ $$ = (Node *)makeString(pstrdup("row")); }
			| func_name_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| reserved_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| qual_all_Op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }

			/* 
			 * For compresstype=none in ENCODING clauses. Allows us to avoid
			 * promoting that to a reserved word or adding the column reserved
			 * list here which could get tricky.
			 */
			| NONE							{ $$ = (Node *)makeString(pstrdup("none")); }
		;

aggr_args:	'(' aggr_args_list ')'					{ $$ = $2; }
			| '(' '*' ')'							{ $$ = NIL; }
		;

aggr_args_list:
			Typename								{ $$ = list_make1($1); }
			| aggr_args_list ',' Typename			{ $$ = lappend($1, $3); }
		;

old_aggr_definition: '(' old_aggr_list ')'			{ $$ = $2; }
		;

old_aggr_list: old_aggr_elem						{ $$ = list_make1($1); }
			| old_aggr_list ',' old_aggr_elem		{ $$ = lappend($1, $3); }
		;

/*
 * Must use IDENT here to avoid reduce/reduce conflicts; fortunately none of
 * the item names needed in old aggregate definitions are likely to become
 * SQL keywords.
 */
old_aggr_elem:  IDENT '=' def_arg
				{
					$$ = makeDefElem($1, (Node *)$3);
				}
		;


/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE OPERATOR CLASS ...
 *				DROP OPERATOR CLASS ...
 *
 *****************************************************************************/

CreateOpClassStmt:
			CREATE OPERATOR CLASS any_name opt_default FOR TYPE_P Typename
			USING access_method AS opclass_item_list
				{
					CreateOpClassStmt *n = makeNode(CreateOpClassStmt);
					n->opclassname = $4;
					n->isDefault = $5;
					n->datatype = $8;
					n->amname = $10;
					n->items = $12;
					$$ = (Node *) n;
				}
		;

opclass_item_list:
			opclass_item							{ $$ = list_make1($1); }
			| opclass_item_list ',' opclass_item	{ $$ = lappend($1, $3); }
		;

opclass_item:
			OPERATOR Iconst any_operator opt_recheck
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->name = $3;
					n->args = NIL;
					n->number = $2;
					n->recheck = $4;
					$$ = (Node *) n;
				}
			| OPERATOR Iconst any_operator '(' oper_argtypes ')' opt_recheck
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->name = $3;
					n->args = $5;
					n->number = $2;
					n->recheck = $7;
					$$ = (Node *) n;
				}
			| FUNCTION Iconst func_name func_args
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_FUNCTION;
					n->name = $3;
					n->args = extractArgTypes($4);
					n->number = $2;
					$$ = (Node *) n;
				}
			| STORAGE Typename
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_STORAGETYPE;
					n->storedtype = $2;
					$$ = (Node *) n;
				}
		;

opt_default:	DEFAULT	{ $$ = TRUE; }
			| /*EMPTY*/	{ $$ = FALSE; }
		;

opt_recheck:	RECHECK	{ $$ = TRUE; }
			| /*EMPTY*/	{ $$ = FALSE; }
		;


DropOpClassStmt:
			DROP OPERATOR CLASS any_name USING access_method opt_drop_behavior
				{
					RemoveOpClassStmt *n = makeNode(RemoveOpClassStmt);
					n->opclassname = $4;
					n->amname = $6;
					n->behavior = $7;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
			| DROP OPERATOR CLASS IF_P EXISTS any_name USING access_method opt_drop_behavior
				{
					RemoveOpClassStmt *n = makeNode(RemoveOpClassStmt);
					n->opclassname = $6;
					n->amname = $8;
					n->behavior = $9;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP OWNED BY username [, username ...] [ RESTRICT | CASCADE ]
 *		REASSIGN OWNED BY username [, username ...] TO username
 *
 *****************************************************************************/
DropOwnedStmt:
			DROP OWNED BY name_list opt_drop_behavior
			 	{
					DropOwnedStmt *n = makeNode(DropOwnedStmt);
					n->roles = $4;
					n->behavior = $5;
					$$ = (Node *)n;
				}
		;

ReassignOwnedStmt:
			REASSIGN OWNED BY name_list TO name
				{
					ReassignOwnedStmt *n = makeNode(ReassignOwnedStmt);
					n->roles = $4;
					n->newrole = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP itemtype [ IF EXISTS ] itemname [, itemname ...] 
 *           [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/

DropStmt:	DROP drop_type IF_P EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					$$ = (Node *)n;
				}
			| DROP drop_type any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
		;


drop_type:	TABLE									{ $$ = OBJECT_TABLE; }
			| EXTERNAL TABLE						{ $$ = OBJECT_EXTTABLE; }
			| EXTERNAL WEB TABLE					{ $$ = OBJECT_EXTTABLE; }	
			| SEQUENCE								{ $$ = OBJECT_SEQUENCE; }
			| VIEW									{ $$ = OBJECT_VIEW; }
			| INDEX									{ $$ = OBJECT_INDEX; }
			| TYPE_P								{ $$ = OBJECT_TYPE; }
			| DOMAIN_P								{ $$ = OBJECT_DOMAIN; }
			| CONVERSION_P							{ $$ = OBJECT_CONVERSION; }
			| SCHEMA								{ $$ = OBJECT_SCHEMA; }
			| FILESPACE								{ $$ = OBJECT_FILESPACE; }
			| FILESYSTEM							{ $$ = OBJECT_FILESYSTEM; }
			| TABLESPACE							{ $$ = OBJECT_TABLESPACE; }
			| FOREIGN TABLE							{ $$ = OBJECT_FOREIGNTABLE; }
			| PROTOCOL								{ $$ = OBJECT_EXTPROTOCOL; }
		;

any_name_list:
			any_name								{ $$ = list_make1($1); }
			| any_name_list ',' any_name			{ $$ = lappend($1, $3); }
		;

any_name:	ColId						{ $$ = list_make1(makeString($1)); }
			| ColId attrs				{ $$ = lcons(makeString($1), $2); }
		;

attrs:		'.' attr_name
					{ $$ = list_make1(makeString($2)); }
			| attrs '.' attr_name
					{ $$ = lappend($1, makeString($3)); }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				truncate table relname1, relname2, ...
 *
 *****************************************************************************/

TruncateStmt:
			TRUNCATE opt_table qualified_name_list opt_drop_behavior
				{
					TruncateStmt *n = makeNode(TruncateStmt);
					n->relations = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *	The COMMENT ON statement can take different forms based upon the type of
 *	the object associated with the comment. The form of the statement is:
 *
 *	COMMENT ON [ [ DATABASE | DOMAIN | INDEX | SEQUENCE | TABLE | TYPE | VIEW |
 *				   CONVERSION | LANGUAGE | OPERATOR CLASS | LARGE OBJECT |
 *				   CAST | COLUMN | SCHEMA | TABLESPACE | ROLE ] <objname> |
 *				 AGGREGATE <aggname> (arg1, ...) |
 *				 FUNCTION <funcname> (arg1, arg2, ...) |
 *				 OPERATOR <op> (leftoperand_typ, rightoperand_typ) |
 *				 TRIGGER <triggername> ON <relname> |
 *				 CONSTRAINT <constraintname> ON <relname> |
 *				 RULE <rulename> ON <relname> ]
 *			   IS 'text'
 *
 *****************************************************************************/

CommentStmt:
			COMMENT ON comment_type any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->objname = $4;
					n->objargs = NIL;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON AGGREGATE func_name aggr_args IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_AGGREGATE;
					n->objname = $4;
					n->objargs = $5;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON FUNCTION func_name func_args IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_FUNCTION;
					n->objname = $4;
					n->objargs = extractArgTypes($5);
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR any_operator '(' oper_argtypes ')'
			IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPERATOR;
					n->objname = $4;
					n->objargs = $6;
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON CONSTRAINT name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_CONSTRAINT;
					n->objname = lappend($6, makeString($4));
					n->objargs = NIL;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON RULE name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_RULE;
					n->objname = lappend($6, makeString($4));
					n->objargs = NIL;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON RULE name IS comment_text
				{
					/* Obsolete syntax supported for awhile for compatibility */
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_RULE;
					n->objname = list_make1(makeString($4));
					n->objargs = NIL;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON TRIGGER name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TRIGGER;
					n->objname = lappend($6, makeString($4));
					n->objargs = NIL;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR CLASS any_name USING access_method IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPCLASS;
					n->objname = $5;
					n->objargs = list_make1(makeString($7));
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON LARGE_P OBJECT_P NumericOnly IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_LARGEOBJECT;
					n->objname = list_make1($5);
					n->objargs = NIL;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON CAST '(' Typename AS Typename ')' IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_CAST;
					n->objname = list_make1($5);
					n->objargs = list_make1($7);
					n->comment = $10;
					$$ = (Node *) n;
				}
			| COMMENT ON opt_procedural LANGUAGE any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_LANGUAGE;
					n->objname = $5;
					n->objargs = NIL;
					n->comment = $7;
					$$ = (Node *) n;
				}
		;

comment_type:
			COLUMN								{ $$ = OBJECT_COLUMN; }
			| DATABASE							{ $$ = OBJECT_DATABASE; }
			| SCHEMA							{ $$ = OBJECT_SCHEMA; }
			| INDEX								{ $$ = OBJECT_INDEX; }
			| SEQUENCE							{ $$ = OBJECT_SEQUENCE; }
			| TABLE								{ $$ = OBJECT_TABLE; }
			| DOMAIN_P							{ $$ = OBJECT_TYPE; }
			| TYPE_P							{ $$ = OBJECT_TYPE; }
			| VIEW								{ $$ = OBJECT_VIEW; }
			| CONVERSION_P						{ $$ = OBJECT_CONVERSION; }
			| TABLESPACE						{ $$ = OBJECT_TABLESPACE; }
			| ROLE								{ $$ = OBJECT_ROLE; }
			| FILESPACE                         { $$ = OBJECT_FILESPACE; }
			| RESOURCE QUEUE                    { $$ = OBJECT_RESQUEUE; }
		;

comment_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *			fetch/move
 *
 *****************************************************************************/

FetchStmt:	FETCH fetch_direction from_in name
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->portalname = $4;
					n->ismove = FALSE;
					$$ = (Node *)n;
				}
			| FETCH name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					n->portalname = $2;
					n->ismove = FALSE;
					$$ = (Node *)n;
				}
			| MOVE fetch_direction from_in name
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->portalname = $4;
					n->ismove = TRUE;
					$$ = (Node *)n;
				}
			| MOVE name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					n->portalname = $2;
					n->ismove = TRUE;
					$$ = (Node *)n;
				}
		;

fetch_direction:
			/*EMPTY*/
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| NEXT
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| PRIOR
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FIRST_P
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_ABSOLUTE;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| LAST_P
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_ABSOLUTE;
					n->howMany = -1;
					$$ = (Node *)n;
				}
			| ABSOLUTE_P SignedIconst
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_ABSOLUTE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| RELATIVE_P SignedIconst
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_RELATIVE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| SignedIconst
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = $1;
					$$ = (Node *)n;
				}
			| ALL
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| FORWARD
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FORWARD SignedIconst
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| FORWARD ALL
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| BACKWARD
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| BACKWARD SignedIconst
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_BACKWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| BACKWARD ALL
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->direction = FETCH_BACKWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
		;

from_in:	FROM									{}
			| IN_P									{}
		;


/*****************************************************************************
 *
 * GRANT and REVOKE statements
 *
 *****************************************************************************/

GrantStmt:	GRANT privileges ON privilege_target TO grantee_list
			opt_grant_grant_option
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = true;
					n->privileges = $2;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->grant_option = $7;
					$$ = (Node*)n;
				}
		;

RevokeStmt:
			REVOKE privileges ON privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = false;
					n->privileges = $2;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->behavior = $7;
					$$ = (Node *)n;
				}
			| REVOKE GRANT OPTION FOR privileges ON privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = true;
					n->privileges = $5;
					n->objtype = ($7)->objtype;
					n->objects = ($7)->objs;
					n->grantees = $9;
					n->behavior = $10;
					$$ = (Node *)n;
				}
		;


/*
 * A privilege list is represented as a list of strings; the validity of
 * the privilege names gets checked at execution.  This is a bit annoying
 * but we have little choice because of the syntactic conflict with lists
 * of role names in GRANT/REVOKE.  What's more, we have to call out in
 * the "privilege" production any reserved keywords that need to be usable
 * as privilege names.
 */

/* either ALL [PRIVILEGES] or a list of individual privileges */
privileges: privilege_list
				{ $$ = $1; }
			| ALL
				{ $$ = NIL; }
			| ALL PRIVILEGES
				{ $$ = NIL; }
		;

privilege_list:	privilege
					{ $$ = list_make1(makeString($1)); }
			| privilege_list ',' privilege
					{ $$ = lappend($1, makeString($3)); }
		;

privilege:	SELECT									{ $$ = pstrdup($1); }
			| REFERENCES							{ $$ = pstrdup($1); }
			| CREATE								{ $$ = pstrdup($1); }
			| ColId									{ $$ = $1; }
		;


/* Don't bother trying to fold the first two rules into one using
 * opt_table.  You're going to get conflicts.
 */
privilege_target:
			qualified_name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $1;
					$$ = n;
				}
			| TABLE qualified_name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $2;
					$$ = n;
				}
			| SEQUENCE qualified_name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_SEQUENCE;
					n->objs = $2;
					$$ = n;
				}
			| FOREIGN DATA_P WRAPPER name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_FDW;
					n->objs = $4;
					$$ = n;
				}
			| FOREIGN SERVER name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_FOREIGN_SERVER;
					n->objs = $3;
					$$ = n;
				}
			| FUNCTION function_with_argtypes_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_FUNCTION;
					n->objs = $2;
					$$ = n;
				}
			| DATABASE name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_DATABASE;
					n->objs = $2;
					$$ = n;
				}
			| LANGUAGE name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_LANGUAGE;
					n->objs = $2;
					$$ = n;
				}
			| SCHEMA name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_NAMESPACE;
					n->objs = $2;
					$$ = n;
				}
			| TABLESPACE name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_TABLESPACE;
					n->objs = $2;
					$$ = n;
				}
			| PROTOCOL name_list
				{
					PrivTarget *n = makeNode(PrivTarget);
					n->objtype = ACL_OBJECT_EXTPROTOCOL;
					n->objs = $2;
					$$ = n;
				}			
		;


grantee_list:
			grantee									{ $$ = list_make1($1); }
			| grantee_list ',' grantee				{ $$ = lappend($1, $3); }
		;

grantee:	RoleId
				{
					PrivGrantee *n = makeNode(PrivGrantee);
					/* This hack lets us avoid reserving PUBLIC as a keyword*/
					if (strcmp($1, "public") == 0)
						n->rolname = NULL;
					else
						n->rolname = $1;
					$$ = (Node *)n;
				}
			| GROUP_P RoleId
				{
					PrivGrantee *n = makeNode(PrivGrantee);
					/* Treat GROUP PUBLIC as a synonym for PUBLIC */
					if (strcmp($2, "public") == 0)
						n->rolname = NULL;
					else
						n->rolname = $2;
					$$ = (Node *)n;
				}
		;


opt_grant_grant_option:
			WITH GRANT OPTION { $$ = TRUE; }
			| /*EMPTY*/ { $$ = FALSE; }
		;

function_with_argtypes_list:
			function_with_argtypes					{ $$ = list_make1($1); }
			| function_with_argtypes_list ',' function_with_argtypes
													{ $$ = lappend($1, $3); }
		;

function_with_argtypes:
			func_name func_args
				{
					FuncWithArgs *n = makeNode(FuncWithArgs);
					n->funcname = $1;
					n->funcargs = extractArgTypes($2);
					$$ = n;
				}
		;

/*****************************************************************************
 *
 * GRANT and REVOKE ROLE statements
 *
 *****************************************************************************/

GrantRoleStmt:
			GRANT privilege_list TO name_list opt_grant_admin_option opt_granted_by
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = true;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->admin_opt = $5;
					n->grantor = $6;
					$$ = (Node*)n;
				}
		;

RevokeRoleStmt:
			REVOKE privilege_list FROM name_list opt_granted_by opt_drop_behavior
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = false;
					n->admin_opt = false;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->behavior = $6;
					$$ = (Node*)n;
				}
			| REVOKE ADMIN OPTION FOR privilege_list FROM name_list opt_granted_by opt_drop_behavior
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = false;
					n->admin_opt = true;
					n->granted_roles = $5;
					n->grantee_roles = $7;
					n->behavior = $9;
					$$ = (Node*)n;
				}
		;

opt_grant_admin_option: WITH ADMIN OPTION				{ $$ = TRUE; }
			| /*EMPTY*/									{ $$ = FALSE; }
		;

opt_granted_by: GRANTED BY RoleId						{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NULL; }
		;


/*****************************************************************************
 *
 *		QUERY: CREATE INDEX
 *
 * Note: we can't factor CONCURRENTLY into a separate production without
 * making it a reserved word.
 *
 * Note: we cannot put TABLESPACE clause after WHERE clause unless we are
 * willing to make TABLESPACE a fully reserved word.
 *****************************************************************************/

IndexStmt:	CREATE index_opt_unique INDEX index_name
			ON qualified_name access_method_clause '(' index_params ')'
			opt_definition OptTableSpace where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = false;
					n->idxname = $4;
					n->relation = $6;
					n->accessMethod = $7;
					n->indexParams = $9;
					n->options = $11;
					n->tableSpace = $12;
					n->whereClause = $13;
					n->idxOids = NULL;
					$$ = (Node *)n;
				}
			| CREATE index_opt_unique INDEX CONCURRENTLY index_name
			ON qualified_name access_method_clause '(' index_params ')'
			opt_definition OptTableSpace where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = true;
					n->idxname = $5;
					n->relation = $7;
					n->accessMethod = $8;
					n->indexParams = $10;
					n->options = $12;
					n->tableSpace = $13;
					n->whereClause = $14;

                    if (!gp_create_index_concurrently)
					{
						/* MPP-9772, MPP-9773: remove support for
						   CREATE INDEX CONCURRENTLY */
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE INDEX CONCURRENTLY is not supported")));

					}

					$$ = (Node *)n;
				}
		;

index_opt_unique:
			UNIQUE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

access_method_clause:
			USING access_method						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = DEFAULT_INDEX_TYPE; }
		;

index_params:	index_elem							{ $$ = list_make1($1); }
			| index_params ',' index_elem			{ $$ = lappend($1, $3); }
		;

/*
 * Index attributes can be either simple column references, or arbitrary
 * expressions in parens.  For backwards-compatibility reasons, we allow
 * an expression that's just a function call to be written without parens.
 */
index_elem:	ColId opt_class
				{
					$$ = makeNode(IndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->opclass = $2;
				}
			| func_expr opt_class
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $1;
					$$->opclass = $2;
				}
			| '(' a_expr ')' opt_class
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $2;
					$$->opclass = $4;
				}
		;

opt_class:	any_name								{ $$ = $1; }
			| USING any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				create [or replace] function <fname>
 *						[(<type-1> { , <type-n>})]
 *						returns <type-r>
 *						as <filename or code in language as appropriate>
 *						language <lang> [with parameters]
 *
 *****************************************************************************/

CreateFunctionStmt:
			CREATE opt_or_replace FUNCTION func_name func_args
			RETURNS func_return createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = $7;
					n->options = $8;
					n->withClause = $9;
					n->funcOid = 0;
					n->shelltypeOid = 0;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name func_args
			  RETURNS TABLE '(' table_func_column_list ')' 
              createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = mergeTableFuncParameters($5, $9);
					n->returnType = TableFuncTypeName($9);
					n->returnType->location = @7;
					n->options = $11;
					n->withClause = $12;
					n->funcOid = 0;
					n->shelltypeOid = 0;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name func_args
			  createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = NULL;
					n->options = $6;
					n->withClause = $7;
					n->funcOid = 0;
					n->shelltypeOid = 0;
					$$ = (Node *)n;
				}
		;

opt_or_replace:
			OR REPLACE								{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

func_args:	'(' func_args_list ')'					{ $$ = $2; }
			| '(' ')'								{ $$ = NIL; }
		;

func_args_list:
			func_arg								{ $$ = list_make1($1); }
			| func_args_list ',' func_arg			{ $$ = lappend($1, $3); }
		;

/*
 * The style with arg_class first is SQL99 standard, but Oracle puts
 * param_name first; accept both since it's likely people will try both
 * anyway.  Don't bother trying to save productions by letting arg_class
 * have an empty alternative ... you'll get shift/reduce conflicts.
 *
 * We can catch over-specified arguments here if we want to,
 * but for now better to silently swallow typmod, etc.
 * - thomas 2000-03-22
 */
func_arg:
			arg_class param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $2;
					n->argType = $3;
					n->mode = $1;
					$$ = n;
				}
			| param_name arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $3;
					n->mode = $2;
					$$ = n;
				}
			| param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_IN;
					$$ = n;
				}
			| arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $2;
					n->mode = $1;
					$$ = n;
				}
			| func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $1;
					n->mode = FUNC_PARAM_IN;
					$$ = n;
				}
		;

/* INOUT is SQL99 standard, IN OUT is for Oracle compatibility */
arg_class:	IN_P									{ $$ = FUNC_PARAM_IN; }
			| OUT_P									{ $$ = FUNC_PARAM_OUT; }
			| INOUT									{ $$ = FUNC_PARAM_INOUT; }
			| IN_P OUT_P							{ $$ = FUNC_PARAM_INOUT; }
		;

/*
 * Ideally param_name should be ColId, but that causes too many conflicts.
 */
param_name:	function_name
		;

func_return:
			func_type
				{
					/* We can catch over-specified results here if we want to,
					 * but for now better to silently swallow typmod, etc.
					 * - thomas 2000-03-22
					 */
					$$ = $1;
				}
		;

/*
 * We would like to make the %TYPE productions here be ColId attrs etc,
 * but that causes reduce/reduce conflicts.  type_name is next best choice.
 */
func_type:	Typename								{ $$ = $1; }
			| type_name attrs '%' TYPE_P
				{
					$$ = makeNode(TypeName);
					$$->names = lcons(makeString($1), $2);
					$$->pct_type = true;
					$$->typmod = -1;
					$$->location = @1;
				}
			| SETOF type_name attrs '%' TYPE_P
				{
					$$ = makeNode(TypeName);
					$$->names = lcons(makeString($2), $3);
					$$->pct_type = true;
					$$->typmod = -1;
					$$->setof = TRUE;
					$$->location = @2;
				}
		;


createfunc_opt_list:
			/* Must be at least one to prevent conflict */
			createfunc_opt_item                     { $$ = list_make1($1); }
			| createfunc_opt_list createfunc_opt_item { $$ = lappend($1, $2); }
	;

/*
 * Options common to both CREATE FUNCTION and ALTER FUNCTION
 */
common_func_opt_item:
			CALLED ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(FALSE));
				}
			| RETURNS NULL_P ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(TRUE));
				}
			| STRICT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(TRUE));
				}
			| IMMUTABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("immutable"));
				}
			| STABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("stable"));
				}
			| VOLATILE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("volatile"));
				}
			| EXTERNAL SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE));
				}
			| EXTERNAL SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE));
				}
			| SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE));
				}
			| SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE));
				}
			| NO SQL
				{
					$$ = makeDefElem("data_access", (Node *)makeString("none"));
				}
			| CONTAINS SQL
				{
					$$ = makeDefElem("data_access", (Node *)makeString("contains"));
				}
			| READS SQL DATA_P
				{
					$$ = makeDefElem("data_access", (Node *)makeString("reads"));
				}
			| MODIFIES SQL DATA_P
				{
					$$ = makeDefElem("data_access", (Node *)makeString("modifies"));
				}
		;

createfunc_opt_item:
			AS func_as
				{
					$$ = makeDefElem("as", (Node *)$2);
				}
			| LANGUAGE ColId_or_Sconst
				{
					$$ = makeDefElem("language", (Node *)makeString($2));
				}
			| common_func_opt_item
				{
					$$ = $1;
				}
		;

func_as:	Sconst						{ $$ = list_make1(makeString($1)); }
			| Sconst ',' Sconst
				{
					$$ = list_make2(makeString($1), makeString($3));
				}
		;

opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

table_func_column:	param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_TABLE;
					$$ = n;
				}
		;

table_func_column_list:
			table_func_column
				{
					$$ = list_make1($1);
				}
			| table_func_column_list ',' table_func_column
				{
					$$ = lappend($1, $3);
				}
		;

/*****************************************************************************
 * ALTER FUNCTION
 *
 * RENAME and OWNER subcommands are already provided by the generic
 * ALTER infrastructure, here we just specify alterations that can
 * only be applied to functions.
 *
 *****************************************************************************/
AlterFunctionStmt:
			ALTER FUNCTION function_with_argtypes alterfunc_opt_list opt_restrict
				{
					AlterFunctionStmt *n = makeNode(AlterFunctionStmt);
					n->func = $3;
					n->actions = $4;
					$$ = (Node *) n;
				}
		;

alterfunc_opt_list:
			/* At least one option must be specified */
			common_func_opt_item					{ $$ = list_make1($1); }
			| alterfunc_opt_list common_func_opt_item { $$ = lappend($1, $2); }
		;

/* Ignored, merely for SQL compliance */
opt_restrict:
			RESTRICT
			| /*EMPTY*/
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP FUNCTION funcname (arg1, arg2, ...) [ RESTRICT | CASCADE ]
 *		DROP AGGREGATE aggname (arg1, ...) [ RESTRICT | CASCADE ]
 *		DROP OPERATOR opname (leftoperand_typ, rightoperand_typ) [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/

RemoveFuncStmt:
			DROP FUNCTION func_name func_args opt_drop_behavior
				{
					RemoveFuncStmt *n = makeNode(RemoveFuncStmt);
					n->kind = OBJECT_FUNCTION;
					n->name = $3;
					n->args = extractArgTypes($4);
					n->behavior = $5;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| DROP FUNCTION IF_P EXISTS func_name func_args opt_drop_behavior
				{
					RemoveFuncStmt *n = makeNode(RemoveFuncStmt);
					n->kind = OBJECT_FUNCTION;
					n->name = $5;
					n->args = extractArgTypes($6);
					n->behavior = $7;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

RemoveAggrStmt:
			DROP AGGREGATE func_name aggr_args opt_drop_behavior
				{
					RemoveFuncStmt *n = makeNode(RemoveFuncStmt);
					n->kind = OBJECT_AGGREGATE;
					n->name = $3;
					n->args = $4;
					n->behavior = $5;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| DROP AGGREGATE IF_P EXISTS func_name aggr_args opt_drop_behavior
				{
					RemoveFuncStmt *n = makeNode(RemoveFuncStmt);
					n->kind = OBJECT_AGGREGATE;
					n->name = $5;
					n->args = $6;
					n->behavior = $7;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

RemoveOperStmt:
			DROP OPERATOR any_operator '(' oper_argtypes ')' opt_drop_behavior
				{
					RemoveFuncStmt *n = makeNode(RemoveFuncStmt);
					n->kind = OBJECT_OPERATOR;
					n->name = $3;
					n->args = $5;
					n->behavior = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| DROP OPERATOR IF_P EXISTS any_operator '(' oper_argtypes ')' opt_drop_behavior
				{
					RemoveFuncStmt *n = makeNode(RemoveFuncStmt);
					n->kind = OBJECT_OPERATOR;
					n->name = $5;
					n->args = $7;
					n->behavior = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

oper_argtypes:
			Typename
				{
				   ereport(ERROR,
						   (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("missing argument"),
							errhint("Use NONE to denote the missing argument of a unary operator."),
                            errOmitLocation(true),
							scanner_errposition(@1)));
				}
			| Typename ',' Typename
					{ $$ = list_make2($1, $3); }
			| NONE ',' Typename /* left unary */
					{ $$ = list_make2(NULL, $3); }
			| Typename ',' NONE /* right unary */
					{ $$ = list_make2($1, NULL); }
		;

any_operator:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| ColId '.' any_operator
					{ $$ = lcons(makeString($1), $3); }
		;


/*****************************************************************************
 *
 *		CREATE CAST / DROP CAST
 *
 *****************************************************************************/

CreateCastStmt: CREATE CAST '(' Typename AS Typename ')'
					WITH FUNCTION function_with_argtypes cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = $10;
					n->context = (CoercionContext) $11;
					$$ = (Node *)n;
				}
			| CREATE CAST '(' Typename AS Typename ')'
					WITHOUT FUNCTION cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = NULL;
					n->context = (CoercionContext) $10;
					$$ = (Node *)n;
				}
		;

cast_context:  AS IMPLICIT_P					{ $$ = COERCION_IMPLICIT; }
		| AS ASSIGNMENT							{ $$ = COERCION_ASSIGNMENT; }
		| /*EMPTY*/								{ $$ = COERCION_EXPLICIT; }
		;


DropCastStmt: DROP CAST opt_if_exists '(' Typename AS Typename ')' opt_drop_behavior
				{
					DropCastStmt *n = makeNode(DropCastStmt);
					n->sourcetype = $5;
					n->targettype = $7;
					n->behavior = $9;
					n->missing_ok = $3;
					$$ = (Node *)n;
				}
		;

opt_if_exists: IF_P EXISTS						{ $$ = true; }
		| /*EMPTY*/								{ $$ = false; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		REINDEX type <name> [FORCE]
 *
 * FORCE no longer does anything, but we accept it for backwards compatibility
 *****************************************************************************/

ReindexStmt:
			REINDEX reindex_type qualified_name opt_force
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = $2;
					n->relation = $3;
					n->name = NULL;
					$$ = (Node *)n;
				}
			| REINDEX SYSTEM_P name opt_force
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = OBJECT_DATABASE;
					n->name = $3;
					n->relation = NULL;
					n->do_system = true;
					n->do_user = false;
					$$ = (Node *)n;
				}
			| REINDEX DATABASE name opt_force
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = OBJECT_DATABASE;
					n->name = $3;
					n->relation = NULL;
					n->do_system = true;
					n->do_user = true;
					$$ = (Node *)n;
				}
		;

reindex_type:
			INDEX									{ $$ = OBJECT_INDEX; }
			| TABLE									{ $$ = OBJECT_TABLE; }
		;

opt_force:	FORCE									{  $$ = TRUE; }
			| /*EMPTY*/								{  $$ = FALSE; }
		;

/*
 * ALTER TYPE ... SET DEFAULT ENCODING
 *
 * Used to set storage parameter defaults for types.
 */
AlterTypeStmt: ALTER TYPE_P SimpleTypename SET DEFAULT ENCODING definition
				{
					AlterTypeStmt *n = makeNode(AlterTypeStmt);

					n->typname = $3;
					n->encoding = $7;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER THING name RENAME TO newname
 *
 *****************************************************************************/

RenameStmt: ALTER AGGREGATE func_name aggr_args RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_AGGREGATE;
					n->object = $3;
					n->objarg = $4;
					n->newname = $7;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_CONVERSION;
					n->object = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER DATABASE database_name RENAME TO database_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_DATABASE;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER FILESPACE name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FILESPACE;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER FILESYSTEM name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FILESYSTEM;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION func_name func_args RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FUNCTION;
					n->object = $3;
					n->objarg = extractArgTypes($4);
					n->newname = $7;
					$$ = (Node *)n;
				}
			| ALTER GROUP_P RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER LANGUAGE name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_LANGUAGE;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS any_name USING access_method RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_OPCLASS;
					n->object = $4;
					n->subname = $6;
					n->newname = $9;
					$$ = (Node *)n;
				}
			| ALTER SCHEMA name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SCHEMA;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER INDEX relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					$$ = (Node *)n;
				}
			| ALTER TRIGGER name ON relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TRIGGER;
					n->relation = $5;
					n->subname = $3;
					n->newname = $8;
					$$ = (Node *)n;
				}
			| ALTER ROLE RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER USER RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLESPACE;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			| ALTER PROTOCOL name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_EXTPROTOCOL;
					n->subname = $3;
					n->newname = $6;
					$$ = (Node *)n;
				}
			
		;

opt_column: COLUMN									{ $$ = COLUMN; }
			| /*EMPTY*/								{ $$ = 0; }
		;

/*****************************************************************************
 *
 * ALTER THING name SET SCHEMA name
 *
 *****************************************************************************/

AlterObjectSchemaStmt:
			ALTER AGGREGATE func_name aggr_args SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_AGGREGATE;
					n->object = $3;
					n->objarg = $4;
					n->newschema = $7;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_DOMAIN;
					n->object = $3;
					n->newschema = $6;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION func_name func_args SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3;
					n->objarg = extractArgTypes($4);
					n->newschema = $7;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_SEQUENCE;
					n->relation = $3;
					n->newschema = $6;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TABLE;
					n->relation = $3;
					n->newschema = $6;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P SimpleTypename SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TYPE;
					n->object = $3->names;
					n->newschema = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER THING name OWNER TO newname
 *
 *****************************************************************************/

AlterOwnerStmt: ALTER AGGREGATE func_name aggr_args OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_AGGREGATE;
					n->object = $3;
					n->objarg = $4;
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_CONVERSION;
					n->object = $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER DATABASE database_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DATABASE;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DOMAIN;
					n->object = $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER FILESPACE name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FILESPACE;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER FILESYSTEM name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FILESYSTEM;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION func_name func_args OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3;
					n->objarg = extractArgTypes($4);
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR any_operator '(' oper_argtypes ')' OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPERATOR;
					n->object = $3;
					n->objarg = $5;
					n->newowner = $9;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS any_name USING access_method OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPCLASS;
					n->object = $4;
					n->addname = $6;
					n->newowner = $9;
					$$ = (Node *)n;
				}
			| ALTER SCHEMA name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_SCHEMA;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P SimpleTypename OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TYPE;
					n->object = $3->names;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TABLESPACE;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FDW;
					n->object = list_make1(makeString($5));
					n->newowner = $8;
					$$ = (Node *)n;
				}
			| ALTER SERVER name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FOREIGN_SERVER;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER PROTOCOL name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_EXTPROTOCOL;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:	Define Rewrite Rule
 *
 *****************************************************************************/

RuleStmt:	CREATE opt_or_replace RULE name AS
			{ QueryIsRule=TRUE; }
			ON event TO qualified_name where_clause
			DO opt_instead RuleActionList
				{
					RuleStmt *n = makeNode(RuleStmt);
					n->replace = $2;
					n->relation = $10;
					n->rulename = $4;
					n->whereClause = $11;
					n->event = $8;
					n->instead = $13;
					n->actions = $14;
					$$ = (Node *)n;
					QueryIsRule=FALSE;
				}
		;

RuleActionList:
			NOTHING									{ $$ = NIL; }
			| RuleActionStmt						{ $$ = list_make1($1); }
			| '(' RuleActionMulti ')'				{ $$ = $2; }
		;

/* the thrashing around here is to discard "empty" statements... */
RuleActionMulti:
			RuleActionMulti ';' RuleActionStmtOrEmpty
				{ if ($3 != NULL)
					$$ = lappend($1, $3);
				  else
					$$ = $1;
				}
			| RuleActionStmtOrEmpty
				{ if ($1 != NULL)
					$$ = list_make1($1);
				  else
					$$ = NIL;
				}
		;

RuleActionStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| NotifyStmt
		;

RuleActionStmtOrEmpty:
			RuleActionStmt							{ $$ = $1; }
			|	/*EMPTY*/							{ $$ = NULL; }
		;

event:		SELECT									{ $$ = CMD_SELECT; }
			| UPDATE								{ $$ = CMD_UPDATE; }
			| DELETE_P								{ $$ = CMD_DELETE; }
			| INSERT								{ $$ = CMD_INSERT; }
		 ;

opt_instead:
			INSTEAD									{ $$ = TRUE; }
			| ALSO									{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;


DropRuleStmt:
			DROP RULE name ON qualified_name opt_drop_behavior
				{
					DropPropertyStmt *n = makeNode(DropPropertyStmt);
					n->relation = $5;
					n->property = $3;
					n->behavior = $6;
					n->removeType = OBJECT_RULE;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
			| DROP RULE IF_P EXISTS name ON qualified_name opt_drop_behavior
				{
					DropPropertyStmt *n = makeNode(DropPropertyStmt);
					n->relation = $7;
					n->property = $5;
					n->behavior = $8;
					n->removeType = OBJECT_RULE;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:
 *				NOTIFY <qualified_name> can appear both in rule bodies and
 *				as a query-level command
 *
 *****************************************************************************/

NotifyStmt: NOTIFY qualified_name
				{
					NotifyStmt *n = makeNode(NotifyStmt);
					n->relation = $2;
					$$ = (Node *)n;
				}
		;

ListenStmt: LISTEN qualified_name
				{
					ListenStmt *n = makeNode(ListenStmt);
					n->relation = $2;
					$$ = (Node *)n;
				}
		;

UnlistenStmt:
			UNLISTEN qualified_name
				{
					UnlistenStmt *n = makeNode(UnlistenStmt);
					n->relation = $2;
					$$ = (Node *)n;
				}
			| UNLISTEN '*'
				{
					UnlistenStmt *n = makeNode(UnlistenStmt);
					n->relation = makeNode(RangeVar);
					n->relation->relname = "*";
					n->relation->schemaname = NULL;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		Transactions:
 *
 *		BEGIN / COMMIT / ROLLBACK
 *		(also older versions END / ABORT)
 *
 *****************************************************************************/

TransactionStmt:
			ABORT_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| BEGIN_P opt_transaction transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					n->options = $3;
					$$ = (Node *)n;
				}
			| START TRANSACTION transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					n->options = $3;
					$$ = (Node *)n;
				}
			| COMMIT opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| END_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_SAVEPOINT;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| RELEASE SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($3)));
					$$ = (Node *)n;
				}
			| RELEASE ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($5)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($4)));
					$$ = (Node *)n;
				}
			| PREPARE TRANSACTION Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_PREPARE;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
		;

opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;

transaction_mode_item:
			ISOLATION LEVEL iso_level
					{ $$ = makeDefElem("transaction_isolation",
									   makeStringConst($3, NULL, @3)); }
			| READ ONLY
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(TRUE, @1)); }
			| READ WRITE
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(FALSE, @1)); }
		;

/* Syntax with commas is SQL-spec, without commas is Postgres historical */
transaction_mode_list:
			transaction_mode_item
					{ $$ = list_make1($1); }
			| transaction_mode_list ',' transaction_mode_item
					{ $$ = lappend($1, $3); }
			| transaction_mode_list transaction_mode_item
					{ $$ = lappend($1, $2); }
		;

transaction_mode_list_or_empty:
			transaction_mode_list
			| /*EMPTY*/
					{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *	QUERY:
 *		CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
 *			AS <query> [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
 *
 *****************************************************************************/

ViewStmt: CREATE OptTemp VIEW qualified_name opt_column_list
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->relOid = 0;
					n->replace = false;
					n->view = $4;
					n->view->istemp = $2;
					n->aliases = $5;
					n->query = (Query *) $7;
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->replace = true;
					n->view = $6;
					n->view->istemp = $4;
					n->aliases = $7;
					n->query = (Query *) $9;
					$$ = (Node *) n;
				}
		;

/*
 * We use merged tokens here to avoid creating shift/reduce conflicts against
 * a whole lot of other uses of WITH.
 */
opt_check_option:
		WITH_CHECK OPTION
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH CHECK OPTION is not implemented")));
				}
		| WITH_CASCADED CHECK OPTION
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH CHECK OPTION is not implemented")));
				}
		| WITH_LOCAL CHECK OPTION
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH CHECK OPTION is not implemented")));
				}
		| /*EMPTY*/							{ $$ = NIL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				LOAD "filename"
 *
 *****************************************************************************/

LoadStmt:	LOAD file_name
				{
					LoadStmt *n = makeNode(LoadStmt);
					n->filename = $2;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		CREATE DATABASE
 *
 *****************************************************************************/

CreatedbStmt:
			CREATE DATABASE database_name opt_with createdb_opt_list
				{
					CreatedbStmt *n = makeNode(CreatedbStmt);
					n->dbname = $3;
					n->options = $5;
					n->dbOid = 0;
					$$ = (Node *)n;
				}
		;

createdb_opt_list:
			createdb_opt_list createdb_opt_item		{ $$ = lappend($1, $2); }
			| /*EMPTY*/							{ $$ = NIL; }
		;

createdb_opt_item:
			TABLESPACE opt_equal name
				{
					$$ = makeDefElem("tablespace", (Node *)makeString($3));
				}
			| TABLESPACE opt_equal DEFAULT
				{
					$$ = makeDefElem("tablespace", NULL);
				}
			| LOCATION opt_equal Sconst
				{
					$$ = makeDefElem("location", (Node *)makeString($3));
				}
			| LOCATION opt_equal DEFAULT
				{
					$$ = makeDefElem("location", NULL);
				}
			| TEMPLATE opt_equal name
				{
					$$ = makeDefElem("template", (Node *)makeString($3));
				}
			| TEMPLATE opt_equal DEFAULT
				{
					$$ = makeDefElem("template", NULL);
				}
			| ENCODING opt_equal Sconst
				{
					$$ = makeDefElem("encoding", (Node *)makeString($3));
				}
			| ENCODING opt_equal Iconst
				{
					$$ = makeDefElem("encoding", (Node *)makeInteger($3));
				}
			| ENCODING opt_equal DEFAULT
				{
					$$ = makeDefElem("encoding", NULL);
				}
			| CONNECTION LIMIT opt_equal SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($4));
				}
			| OWNER opt_equal name
				{
					$$ = makeDefElem("owner", (Node *)makeString($3));
				}
			| OWNER opt_equal DEFAULT
				{
					$$ = makeDefElem("owner", NULL);
				}
		;

/*
 *	Though the equals sign doesn't match other WITH options, pg_dump uses
 *	equals for backward compatibility, and it doesn't seem worth removing it.
 */
opt_equal:	'='										{}
			| /*EMPTY*/								{}
		;


/*****************************************************************************
 *
 *		ALTER DATABASE
 *
 *****************************************************************************/

AlterDatabaseStmt:
			ALTER DATABASE database_name opt_with alterdb_opt_list
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = $5;
					$$ = (Node *)n;
				 }
		;

AlterDatabaseSetStmt:
			ALTER DATABASE database_name SET set_rest
				{
					AlterDatabaseSetStmt *n = makeNode(AlterDatabaseSetStmt);
					n->dbname = $3;
					n->variable = $5->name;
					n->value = $5->args;
					$$ = (Node *)n;
				}
			| ALTER DATABASE database_name VariableResetStmt
				{
					AlterDatabaseSetStmt *n = makeNode(AlterDatabaseSetStmt);
					n->dbname = $3;
					n->variable = ((VariableResetStmt *)$4)->name;
					n->value = NIL;
					$$ = (Node *)n;
				}
		;


alterdb_opt_list:
			alterdb_opt_list alterdb_opt_item		{ $$ = lappend($1, $2); }
			| /*EMPTY*/							{ $$ = NIL; }
		;

alterdb_opt_item:
			CONNECTION LIMIT opt_equal SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($4));
				}
		;


/*****************************************************************************
 *
 *		DROP DATABASE [ IF EXISTS ]
 *
 * This is implicitly CASCADE, no need for drop behavior
 *****************************************************************************/

DropdbStmt: DROP DATABASE database_name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $3;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			| DROP DATABASE IF_P EXISTS database_name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $5;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Manipulate a domain
 *
 *****************************************************************************/

CreateDomainStmt:
			CREATE DOMAIN_P any_name opt_as Typename ColQualList
				{
					CreateDomainStmt *n = makeNode(CreateDomainStmt);
					n->domainname = $3;
					n->typname = $5;
					n->constraints = $6;
					n->domainOid = 0;
					$$ = (Node *)n;
				}
		;

AlterDomainStmt:
			/* ALTER DOMAIN <domain> {SET DEFAULT <expr>|DROP DEFAULT} */
			ALTER DOMAIN_P any_name alter_column_default
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'T';
					n->typname = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP NOT NULL */
			| ALTER DOMAIN_P any_name DROP NOT NULL_P
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'N';
					n->typname = $3;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> SET NOT NULL */
			| ALTER DOMAIN_P any_name SET NOT NULL_P
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'O';
					n->typname = $3;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> ADD CONSTRAINT ... */
			| ALTER DOMAIN_P any_name ADD_P TableConstraint
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'C';
					n->typname = $3;
					n->def = $5;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| ALTER DOMAIN_P any_name DROP CONSTRAINT name opt_drop_behavior
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'X';
					n->typname = $3;
					n->name = $6;
					n->behavior = $7;
					$$ = (Node *)n;
				}
			;

opt_as:		AS										{}
			| /*EMPTY*/							{}
		;


/*****************************************************************************
 *
 * Manipulate a conversion
 *
 *		CREATE [DEFAULT] CONVERSION <conversion_name>
 *		FOR <encoding_name> TO <encoding_name> FROM <func_name>
 *
 *****************************************************************************/

CreateConversionStmt:
			CREATE opt_default CONVERSION_P any_name FOR Sconst
			TO Sconst FROM any_name
			{
			  CreateConversionStmt *n = makeNode(CreateConversionStmt);
			  n->conversion_name = $4;
			  n->for_encoding_name = $6;
			  n->to_encoding_name = $8;
			  n->func_name = $10;
			  n->def = $2;
			  $$ = (Node *)n;
			}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CLUSTER [VERBOSE] <qualified_name> [ USING <index_name> ]
 *				CLUSTER [VERBOSE]
 *				CLUSTER [VERBOSE] <index_name> ON <qualified_name> (for pre-8.3)
 *
 *****************************************************************************/

ClusterStmt:
			CLUSTER opt_verbose index_name ON qualified_name
				{
				   ClusterStmt *n = makeNode(ClusterStmt);
				   n->relation = $5;
				   n->indexname = $3;
				   $$ = (Node*)n;
				}
			| CLUSTER opt_verbose qualified_name
				{
			       ClusterStmt *n = makeNode(ClusterStmt);
				   n->relation = $3;
				   n->indexname = NULL;
				   $$ = (Node*)n;
				}
			| CLUSTER opt_verbose
			    {
				   ClusterStmt *n = makeNode(ClusterStmt);
				   n->relation = NULL;
				   n->indexname = NULL;
				   $$ = (Node*)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				VACUUM
 *				ANALYZE
 *
 *****************************************************************************/

VacuumStmt: VACUUM opt_full opt_freeze opt_verbose
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->vacuum = true;
					n->analyze = false;
					n->full = $2;
					n->freeze_min_age = $3 ? 0 : -1;
					n->verbose = $4;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose qualified_name
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->vacuum = true;
					n->analyze = false;
					n->full = $2;
					n->freeze_min_age = $3 ? 0 : -1;
					n->verbose = $4;
					n->relation = $5;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose AnalyzeStmt
				{
					VacuumStmt *n = (VacuumStmt *) $5;
					n->vacuum = true;
					n->full = $2;
					n->freeze_min_age = $3 ? 0 : -1;
					n->verbose |= $4;
					$$ = (Node *)n;
				}
		;

AnalyzeStmt:
			analyze_keyword opt_verbose opt_rootonly_all
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->vacuum = false;
					n->analyze = true;
					n->full = false;
					n->freeze_min_age = -1;
					n->verbose = $2;
					n->rootonly = $3;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| analyze_keyword opt_verbose qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->vacuum = false;
					n->analyze = true;
					n->full = false;
					n->freeze_min_age = -1;
					n->verbose = $2;
					n->rootonly = false;
					n->relation = $3;
					n->va_cols = $4;
					$$ = (Node *)n;
				}
			| analyze_keyword opt_verbose ROOTPARTITION qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->vacuum = false;
					n->analyze = true;
					n->full = false;
					n->freeze_min_age = -1;
					n->verbose = $2;
					n->rootonly = true;
					n->relation = $4;
					n->va_cols = $5;
					$$ = (Node *)n;
				}
		;

analyze_keyword:
			ANALYZE									{}
			| ANALYSE /* British */					{}
		;

opt_verbose:
			VERBOSE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_rootonly_all:
			ROOTPARTITION ALL						{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;
			
opt_full:	FULL									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_freeze: FREEZE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_name_list:
			'(' name_list ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				EXPLAIN [ANALYZE] [VERBOSE] query
 *
 *****************************************************************************/

ExplainStmt: EXPLAIN opt_analyze opt_verbose opt_dxl opt_force ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->analyze = $2;
					n->verbose = $3;
					n->dxl = $4;
					if($5)
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
								errmsg("cannot use force with explain statement")
							       ));
					n->query = (Query*)$6;
					$$ = (Node *)n;
				}
		;

ExplainableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| DeclareCursorStmt
			| ExecuteStmt					/* by default all are $$=$1 */
			| CreateAsStmt
			| CreateStmt
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot EXPLAIN CREATE TABLE without AS "
							 		"clause")));
				}
		;

opt_dxl:	DXL										{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_analyze:
			analyze_keyword			{ $$ = TRUE; }
			| /*EMPTY*/			{ $$ = FALSE; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				PREPARE <plan_name> [(args, ...)] AS <query>
 *
 *****************************************************************************/

PrepareStmt: PREPARE name prep_type_clause AS PreparableStmt
				{
					PrepareStmt *n = makeNode(PrepareStmt);
					n->name = $2;
					n->argtypes = $3;
					n->query = (Query *) $5;
					$$ = (Node *) n;
				}
		;

prep_type_clause: '(' prep_type_list ')'	{ $$ = $2; }
				| /*EMPTY*/				{ $$ = NIL; }
		;

prep_type_list: Typename			{ $$ = list_make1($1); }
			  | prep_type_list ',' Typename
									{ $$ = lappend($1, $3); }
		;

PreparableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt					/* by default all are $$=$1 */
		;

/*****************************************************************************
 *
 * EXECUTE <plan_name> [(params, ...)]
 * CREATE TABLE <name> AS EXECUTE <plan_name> [(params, ...)]
 *
 *****************************************************************************/

ExecuteStmt: EXECUTE name execute_param_clause
				{
					ExecuteStmt *n = makeNode(ExecuteStmt);
					n->name = $2;
					n->params = $3;
					n->into = NULL;
					$$ = (Node *) n;
				}
			| CREATE OptTemp TABLE create_as_target AS
				EXECUTE name execute_param_clause
				{
					ExecuteStmt *n = makeNode(ExecuteStmt);
					n->name = $7;
					n->params = $8;
					$4->rel->istemp = $2;
					n->into = $4;
					if ($4->colNames)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("column name list not allowed in CREATE TABLE / AS EXECUTE")));
					/* ... because it's not implemented, but it could be */
					$$ = (Node *) n;
				}
		;

execute_param_clause: '(' expr_list ')'				{ $$ = $2; }
					| /*EMPTY*/					{ $$ = NIL; }
					;

/*****************************************************************************
 *
 *		QUERY:
 *				DEALLOCATE [PREPARE] <plan_name>
 *
 *****************************************************************************/

DeallocateStmt: DEALLOCATE name
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = $2;
						$$ = (Node *) n;
					}
				| DEALLOCATE PREPARE name
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = $3;
						$$ = (Node *) n;
					}
		;

/*****************************************************************************
 *
 */




cdb_string_list:
			cdb_string							{ $$ = list_make1($1); }  
			| cdb_string_list ',' cdb_string	{ $$ = lappend($1, $3); }
		;


cdb_string:
			Sconst
				{
					$$ = (Node *) makeString($1);
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				INSERT STATEMENTS
 *
 *****************************************************************************/

InsertStmt:
			INSERT INTO qualified_name insert_rest returning_clause
				{
					$4->relation = $3;
					$4->returningList = $5;
					$$ = (Node *) $4;
				}
		;

insert_rest:
			SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = $1;
				}
			| '(' insert_column_list ')' SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = $2;
					$$->selectStmt = $4;
				}
			| DEFAULT VALUES
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = NULL;
				}
		;

insert_column_list:
			insert_column_item
					{ $$ = list_make1($1); }
			| insert_column_list ',' insert_column_item
					{ $$ = lappend($1, $3); }
		;

insert_column_item:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = $2;
					$$->val = NULL;
					$$->location = @1;
				}
		;

returning_clause:
			RETURNING target_list		{ $$ = $2; }
			| /*EMPTY*/				{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				DELETE STATEMENTS
 *
 *****************************************************************************/

DeleteStmt: DELETE_P FROM relation_expr_opt_alias
			using_clause where_or_current_clause returning_clause
				{
					DeleteStmt *n = makeNode(DeleteStmt);
					n->relation = $3;
					n->usingClause = $4;
					n->whereClause = $5;
					n->returningList = $6;
					$$ = (Node *)n;
				}
		;

using_clause:
	    		USING from_list						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				LOCK TABLE
 *
 *****************************************************************************/

LockStmt:	LOCK_P opt_table qualified_name_list opt_lock opt_nowait
				{
					LockStmt *n = makeNode(LockStmt);

					n->relations = $3;
					n->mode = $4;
					n->nowait = $5;
					$$ = (Node *)n;
				}
		;

opt_lock:	IN_P lock_type MODE 			{ $$ = $2; }
			| /*EMPTY*/						{ $$ = AccessExclusiveLock; }
		;

lock_type:	ACCESS SHARE					{ $$ = AccessShareLock; }
			| ROW SHARE						{ $$ = RowShareLock; }
			| ROW EXCLUSIVE					{ $$ = RowExclusiveLock; }
			| SHARE UPDATE EXCLUSIVE		{ $$ = ShareUpdateExclusiveLock; }
			| SHARE							{ $$ = ShareLock; }
			| SHARE ROW EXCLUSIVE			{ $$ = ShareRowExclusiveLock; }
			| EXCLUSIVE						{ $$ = ExclusiveLock; }
			| ACCESS EXCLUSIVE				{ $$ = AccessExclusiveLock; }
		;

opt_nowait:	NOWAIT							{ $$ = TRUE; }
			| /*EMPTY*/						{ $$ = FALSE; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				UpdateStmt (UPDATE)
 *
 *****************************************************************************/

UpdateStmt: UPDATE relation_expr_opt_alias
			SET set_clause_list
			from_clause
			where_or_current_clause
			returning_clause
				{
					UpdateStmt *n = makeNode(UpdateStmt);
					n->relation = $2;
					n->targetList = $4;
					n->fromClause = $5;
					n->whereClause = $6;
					n->returningList = $7;
					$$ = (Node *)n;
				}
		;

set_clause_list:
			set_clause							{ $$ = $1; }
			| set_clause_list ',' set_clause	{ $$ = list_concat($1,$3); }
		;

set_clause:
			single_set_clause						{ $$ = list_make1($1); }
			| multiple_set_clause					{ $$ = $1; }
		;

single_set_clause:
			set_target '=' ctext_expr
				{
					$$ = $1;
					$$->val = (Node *) $3;
				}
		;

multiple_set_clause:
			'(' set_target_list ')' '=' ctext_row
				{
					ListCell *col_cell;
					ListCell *val_cell;

					/*
					 * Break the ctext_row apart, merge individual expressions
					 * into the destination ResTargets.  XXX this approach
					 * cannot work for general row expressions as sources.
					 */
					if (list_length($2) != list_length($5))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("number of columns does not match number of values"),
								 scanner_errposition(@1)));
					forboth(col_cell, $2, val_cell, $5)
					{
						ResTarget *res_col = (ResTarget *) lfirst(col_cell);
						Node *res_val = (Node *) lfirst(val_cell);

						res_col->val = res_val;
					}
				    
					$$ = $2;
				}
		;

set_target:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = $2;
					$$->val = NULL;	/* upper production sets this */
					$$->location = @1;
				}
		;

set_target_list:
			set_target								{ $$ = list_make1($1); }
			| set_target_list ',' set_target		{ $$ = lappend($1,$3); }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				CURSOR STATEMENTS
 *
 *****************************************************************************/
DeclareCursorStmt: DECLARE name cursor_options CURSOR opt_hold FOR SelectStmt
				{
					DeclareCursorStmt *n = makeNode(DeclareCursorStmt);
					n->portalname = $2;
					n->options = $3;
					n->query = $7;
					if ($5)
						n->options |= CURSOR_OPT_HOLD;
					$$ = (Node *)n;
				}
		;

cursor_options: /*EMPTY*/					{ $$ = 0; }
			| cursor_options NO SCROLL		{ $$ = $1 | CURSOR_OPT_NO_SCROLL; }
			| cursor_options SCROLL			{ $$ = $1 | CURSOR_OPT_SCROLL; }
			| cursor_options BINARY			{ $$ = $1 | CURSOR_OPT_BINARY; }
			| cursor_options INSENSITIVE	{ $$ = $1 | CURSOR_OPT_INSENSITIVE; }
		;

opt_hold: /*EMPTY*/						{ $$ = FALSE; }
			| WITH HOLD						{ $$ = TRUE; }
			| WITHOUT HOLD					{ $$ = FALSE; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				SELECT STATEMENTS
 *
 *****************************************************************************/

/* A complete SELECT statement looks like this.
 *
 * The rule returns either a single SelectStmt node or a tree of them,
 * representing a set-operation tree.
 *
 * There is an ambiguity when a sub-SELECT is within an a_expr and there
 * are excess parentheses: do the parentheses belong to the sub-SELECT or
 * to the surrounding a_expr?  We don't really care, but yacc wants to know.
 * To resolve the ambiguity, we are careful to define the grammar so that
 * the decision is staved off as long as possible: as long as we can keep
 * absorbing parentheses into the sub-SELECT, we will do so, and only when
 * it's no longer possible to do that will we decide that parens belong to
 * the expression.	For example, in "SELECT (((SELECT 2)) + 3)" the extra
 * parentheses are treated as part of the sub-select.  The necessity of doing
 * it that way is shown by "SELECT (((SELECT 2)) UNION SELECT 2)".	Had we
 * parsed "((SELECT 2))" as an a_expr, it'd be too late to go back to the
 * SELECT viewpoint when we see the UNION.
 *
 * This approach is implemented by defining a nonterminal select_with_parens,
 * which represents a SELECT with at least one outer layer of parentheses,
 * and being careful to use select_with_parens, never '(' SelectStmt ')',
 * in the expression grammar.  We will then have shift-reduce conflicts
 * which we can resolve in favor of always treating '(' <select> ')' as
 * a select_with_parens.  To resolve the conflicts, the productions that
 * conflict with the select_with_parens productions are manually given
 * precedences lower than the precedence of ')', thereby ensuring that we
 * shift ')' (and then reduce to select_with_parens) rather than trying to
 * reduce the inner <select> nonterminal to something else.  We use UMINUS
 * precedence for this, which is a fairly arbitrary choice.
 *
 * To be able to define select_with_parens itself without ambiguity, we need
 * a nonterminal select_no_parens that represents a SELECT structure with no
 * outermost parentheses.  This is a little bit tedious, but it works.
 *
 * In non-expression contexts, we use SelectStmt which can represent a SELECT
 * with or without outer parentheses.
 */

SelectStmt: select_no_parens			%prec UMINUS
			| select_with_parens		%prec UMINUS
		;

select_with_parens:
			'(' select_no_parens ')'				{ $$ = $2; }
			| '(' select_with_parens ')'			{ $$ = $2; }
		;

/*
 * This rule parses the equivalent of the standard's <query expression>.
 * The duplicative productions are annoying, but hard to get rid of without
 * creating shift/reduce conflicts.
 *
 *	FOR UPDATE/SHARE may be before or after LIMIT/OFFSET.
 *	In <=7.2.X, LIMIT/OFFSET had to be after FOR UPDATE
 *	We now support both orderings, but prefer LIMIT/OFFSET before FOR UPDATE/SHARE
 *	2002-08-28 bjm
 */
select_no_parens:
			simple_select						{ $$ = $1; }
			| select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, NIL,
										NULL, NULL, NULL);
					$$ = $1;
				}
			| select_clause opt_sort_clause for_locking_clause opt_select_limit
				{
					insertSelectOptions((SelectStmt *) $1, $2, $3,
										list_nth($4, 0), list_nth($4, 1), NULL);
					$$ = $1;
				}
			| select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, $4,
										list_nth($3, 0), list_nth($3, 1), NULL);
					$$ = $1;
				}
			| with_clause select_clause
				{
					insertSelectOptions((SelectStmt *) $2, NULL, NIL,
										NULL, NULL,
										$1);
					$$ = $2;
				}
			| with_clause select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, NIL,
										NULL, NULL,
										$1);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
				{
					insertSelectOptions((SelectStmt *) $2, $3, $4,
										list_nth($5, 0), list_nth($5, 1),
										$1);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, $5,
										list_nth($4, 0), list_nth($4, 1),
										$1);
					$$ = $2;
				}
		;

select_clause:
			simple_select							{ $$ = $1; }
			| select_with_parens					{ $$ = $1; }
		;

/*
 * This rule parses SELECT statements that can appear within set operations,
 * including UNION, INTERSECT and EXCEPT.  '(' and ')' can be used to specify
 * the ordering of the set operations.	Without '(' and ')' we want the
 * operations to be ordered per the precedence specs at the head of this file.
 *
 * As with select_no_parens, simple_select cannot have outer parentheses,
 * but can have parenthesized subclauses.
 *
 * Note that sort clauses cannot be included at this level --- SQL92 requires
 *		SELECT foo UNION SELECT bar ORDER BY baz
 * to be parsed as
 *		(SELECT foo UNION SELECT bar) ORDER BY baz
 * not
 *		SELECT foo UNION (SELECT bar ORDER BY baz)
 * Likewise FOR UPDATE and LIMIT.  Therefore, those clauses are described
 * as part of the select_no_parens production, not simple_select.
 * This does not limit functionality, because you can reintroduce sort and
 * limit clauses inside parentheses.
 *
 * NOTE: only the leftmost component SelectStmt should have INTO.
 * However, this is not checked by the grammar; parse analysis must check it.
 */
simple_select:
			SELECT opt_distinct target_list
			into_clause from_clause where_clause
			group_clause having_clause window_clause
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->distinctClause = $2;
					n->targetList = $3;
					n->intoClause = $4;
					n->fromClause = $5;
					n->whereClause = $6;
					n->groupClause = $7;
					n->havingClause = $8;
					n->windowClause = $9;
					$$ = (Node *)n;
				}
			| values_clause							{ $$ = $1; }
			| select_clause UNION opt_all select_clause
				{
					$$ = makeSetOp(SETOP_UNION, $3, $1, $4);
				}
			| select_clause INTERSECT opt_all select_clause
				{
					$$ = makeSetOp(SETOP_INTERSECT, $3, $1, $4);
				}
			| select_clause EXCEPT opt_all select_clause
				{
					$$ = makeSetOp(SETOP_EXCEPT, $3, $1, $4);
				}
		;

/*
 * SQL standard WITH clause looks like:
 *
 * WITH [ RECURSIVE ] <query name> [ (<column>,...) ]
 *		AS (query) [ SEARCH or CYCLE clause ]
 *
 * We don't currently support the SEARCH or CYCLE clause.
 */
with_clause:
		WITH cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $2;
				$$->recursive = false;
				$$->location = @1;
			}
		| WITH RECURSIVE cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $3;
				$$->recursive = true;
				$$->location = @1;
			}
		;

cte_list:
		common_table_expr						{ $$ = list_make1($1); }
		| cte_list ',' common_table_expr		{ $$ = lappend($1, $3); }
		;

common_table_expr:  name opt_name_list AS select_with_parens
			{
				CommonTableExpr *n = makeNode(CommonTableExpr);
				n->ctename = $1;
				n->aliascolnames = $2;
				n->ctequery = $4;
				n->location = @1;
				$$ = (Node *) n;
			}
		;

into_clause:
			INTO OptTempTableName
				{
					$$ = makeNode(IntoClause);
					$$->rel = $2;
					$$->colNames = NIL;
					$$->options = NIL;
					$$->onCommit = ONCOMMIT_NOOP;
					$$->tableSpaceName = NULL;
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTemp.
 */
OptTempTableName:
			TEMPORARY opt_table qualified_name
				{
					$$ = $3;
					$$->istemp = true;
				}
			| TEMP opt_table qualified_name
				{
					$$ = $3;
					$$->istemp = true;
				}
			| LOCAL TEMPORARY opt_table qualified_name
				{
					$$ = $4;
					$$->istemp = true;
				}
			| LOCAL TEMP opt_table qualified_name
				{
					$$ = $4;
					$$->istemp = true;
				}
			| GLOBAL TEMPORARY opt_table qualified_name
				{
					$$ = $4;
					$$->istemp = true;
				}
			| GLOBAL TEMP opt_table qualified_name
				{
					$$ = $4;
					$$->istemp = true;
				}
			| TABLE qualified_name
				{
					$$ = $2;
					$$->istemp = false;
				}
			| qualified_name
				{
					$$ = $1;
					$$->istemp = false;
				}
		;

opt_table:	TABLE									{}
			| /*EMPTY*/								{}
		;

opt_all:	ALL										{ $$ = TRUE; }
			| DISTINCT								{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* We use (NIL) as a placeholder to indicate that all target expressions
 * should be placed in the DISTINCT list during parsetree analysis.
 */
opt_distinct:
			DISTINCT								{ $$ = list_make1(NIL); }
			| DISTINCT ON '(' expr_list ')'			{ $$ = $4; }
			| ALL									{ $$ = NIL; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_sort_clause:
			sort_clause								{ $$ = $1;}
			| /*EMPTY*/								{ $$ = NIL; }
		;

sort_clause:
			ORDER BY sortby_list					{ $$ = $3; }
		;

sortby_list:
			sortby									{ $$ = list_make1($1); }
			| sortby_list ',' sortby				{ $$ = lappend($1, $3); }
		;

sortby:		a_expr USING qual_all_Op
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_kind = SORTBY_USING;
					$$->useOp = $3;
				}
			| a_expr ASC
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_kind = SORTBY_ASC;
					$$->useOp = NIL;
				}
			| a_expr DESC
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_kind = SORTBY_DESC;
					$$->useOp = NIL;
				}
			| a_expr
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_kind = SORTBY_ASC;	/* default */
					$$->useOp = NIL;
				}
		;


select_limit:
			LIMIT select_limit_value OFFSET select_offset_value
				{ $$ = list_make2($4, $2); }
			| OFFSET select_offset_value LIMIT select_limit_value
				{ $$ = list_make2($2, $4); }
			| LIMIT select_limit_value
				{ $$ = list_make2(NULL, $2); }
			| OFFSET select_offset_value
				{ $$ = list_make2($2, NULL); }
			| LIMIT select_limit_value ',' select_offset_value
				{
					/* Disabled because it was too confusing, bjm 2002-02-18 */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("LIMIT #,# syntax is not supported"),
							 errhint("Use separate LIMIT and OFFSET clauses."),
							 scanner_errposition(@1)));
				}
			/* SQL:2008 syntax variants */
			| OFFSET select_offset_value2 row_or_rows
				{ $$ = list_make2($2, NULL); }
			| FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY
				{ $$ = list_make2(NULL, $3); }
			| OFFSET select_offset_value2 row_or_rows FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY
				{ $$ = list_make2($2, $6); }
		;

opt_select_limit:
			select_limit							{ $$ = $1; }
			| /*EMPTY*/
					{ $$ = list_make2(NULL,NULL); }
		;

select_limit_value:
			a_expr									{ $$ = $1; }
			| ALL
				{
					/* LIMIT ALL is represented as a NULL constant */
					A_Const *n = makeNode(A_Const);
					n->val.type = T_Null;
					$$ = (Node *)n;
				}
		;

/*
 * Allowing full expressions without parentheses causes various parsing
 * problems with the trailing ROW/ROWS key words.  SQL only calls for
 * constants, so we allow the rest only with parentheses.
 */
opt_select_fetch_first_value:
			SignedIconst		{ $$ = makeIntConst($1, @1); }
			| '(' a_expr ')'	{ $$ = $2; }
			| /*EMPTY*/		{ $$ = makeIntConst(1, -1); }
		;

select_offset_value:
			a_expr									{ $$ = $1; }
		;

/*
 * Again, the trailing ROW/ROWS in this case prevent the full expression
 * syntax.  c_expr is the best we can do.
 */
select_offset_value2:
			c_expr									{ $$ = $1; }
		;

/* noise words */
row_or_rows:
			ROW		{ $$ = 0; }
			| ROWS		{ $$ = 0; }
			;

/* noise words */
first_or_next:
			FIRST_P		{ $$ = 0; }
			| NEXT		{ $$ = 0; }
			;

group_clause:
			GROUP_P BY group_elem_list				{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

group_elem_list:
            group_elem                                { $$ = $1; }
            | group_elem_list ',' group_elem            { $$ = list_concat($1, $3); }
        ;

group_elem:
			a_expr                                  { $$ = list_make1($1); }
			| ROLLUP '(' expr_list ')'
                {
					GroupingClause *n = makeNode(GroupingClause);
					n->groupType = GROUPINGTYPE_ROLLUP;
					n->groupsets = $3;
					$$ = list_make1 ((Node*)n);
				}
            | CUBE '(' expr_list ')'
                {
					GroupingClause *n = makeNode(GroupingClause);
					n->groupType = GROUPINGTYPE_CUBE;
					n->groupsets = $3;
					$$ = list_make1 ((Node*)n);
				}
            | GROUPING SETS '(' group_elem_list ')'
                {
					GroupingClause *n = makeNode(GroupingClause);
					n->groupType = GROUPINGTYPE_GROUPING_SETS;
					n->groupsets = $4;
					$$ = list_make1 ((Node*)n);
				}
            | '(' ')'
                { $$ = list_make1(NIL); }
        ;

having_clause:
			HAVING a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

window_clause:
			WINDOW window_definition_list			{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

window_definition_list: 
			window_name AS '(' window_spec ')'
				{
                    ((WindowSpec *)$4)->name = $1;
                    ((WindowSpec *)$4)->location = @3;
					$$ = list_make1($4);
				}
			| window_definition_list ',' window_name AS '(' window_spec ')'
				{
					((WindowSpec *)$6)->name = $3;
					((WindowSpec *)$6)->location = @5;
					$$ = lappend($1, $6);
				}
		;

window_spec: opt_window_name opt_window_partition_clause 
				opt_window_order_clause opt_window_frame_clause
				{
					WindowSpec *n = makeNode(WindowSpec);
					n->parent = $1;
					n->partition = $2;
					n->order = $3;
					n->frame = (WindowFrame *)$4;
					n->location = -1;
					$$ = (Node *)n;
				}
		;

opt_window_name: window_name { $$ = $1; }
			| /*EMPTY*/ { $$ = NULL; }
		;

opt_window_partition_clause: window_partition_clause { $$ = $1; }
			| /*EMPTY*/ { $$ = NIL; }
		;

window_partition_clause: PARTITION BY sortby_list { $$ = (List *)$3; }
		;

opt_window_order_clause: sort_clause { $$ = $1; }
			| /*EMPTY*/ { $$ = NIL; }
        ;

opt_window_frame_clause: window_frame_clause { $$ = $1; }
		| /*EMPTY*/ { $$ = NULL; }
		;

window_frame_clause: window_frame_units window_frame_extent 
			window_frame_exclusion
				{
					WindowFrame *n = makeNode(WindowFrame);
					n->is_rows = $1;
					setWindowExclude(n, $3);

					if (IsA($2, List))
					{
						List *ex = (List *)$2;

						n->trail = (WindowFrameEdge *)linitial(ex);
						n->lead = (WindowFrameEdge *)lsecond(ex);
						n->is_between = true;
					}
					else
					{
						Assert(IsA($2, WindowFrameEdge));
						n->trail = (WindowFrameEdge *)$2;
						n->lead = NULL;
						n->is_between = false;
					}
					$$ = (Node *)n;
				}
		;

/* units are either rows (true) otherwise false */
window_frame_units: ROWS { $$ = true; }
			| RANGE { $$ = false; }
		;

window_frame_extent:
			window_frame_start { $$ = $1; }
			| window_frame_between { $$ = $1; }
		;

window_frame_start:
			UNBOUNDED PRECEDING
				{
					WindowFrameEdge *n = makeNode(WindowFrameEdge);
					n->kind = WINDOW_UNBOUND_PRECEDING;
					n->val = NULL;
					$$ = (Node *)n;
				}
			| window_frame_preceding
				{
					WindowFrameEdge *n = makeNode(WindowFrameEdge);
					n->kind = WINDOW_BOUND_PRECEDING;
					n->val = $1;
					$$ = (Node *)n;
				}
			| CURRENT ROW
				{
					WindowFrameEdge *n = makeNode(WindowFrameEdge);
					n->kind = WINDOW_CURRENT_ROW;
					$$ = (Node *)n;
				}
		;

window_frame_preceding: a_expr PRECEDING 
				{ 
					$$ = (Node *)$1;
				}
		;

window_frame_between: 
			BETWEEN window_frame_bound AND window_frame_bound
				{
					/* slightly dodgy hack */
					$$ = (Node *)list_make2($2, $4);
				}
		;

/*
 * Be careful that we don't allow BETWEEN UNBOUND PRECEDING AND
 * UNBOUND PRECEDING
 */

window_frame_bound:
			window_frame_start { $$ = $1; }
			| UNBOUNDED FOLLOWING 
				{
					WindowFrameEdge *n = makeNode(WindowFrameEdge);
					n->kind = WINDOW_UNBOUND_FOLLOWING;
					n->val = NULL;
					$$ = (Node *)n;
				}
			| window_frame_following
				{
					WindowFrameEdge *n = makeNode(WindowFrameEdge);
					n->kind = WINDOW_BOUND_FOLLOWING;
					n->val = $1;
					$$ = (Node *)n;
				}
		;

window_frame_following: a_expr FOLLOWING 
				{ 
					$$ = (Node *)$1;
				}
		;

window_frame_exclusion: EXCLUDE CURRENT ROW { $$ = WINDOW_EXCLUSION_CUR_ROW; }
			| EXCLUDE GROUP_P { $$ = WINDOW_EXCLUSION_GROUP; }
			| EXCLUDE TIES  { $$ = WINDOW_EXCLUSION_TIES; }
			| EXCLUDE NO OTHERS { $$ = WINDOW_EXCLUSION_NO_OTHERS; }
			| /*EMPTY*/ { $$ = WINDOW_EXCLUSION_NULL; }
		;

for_locking_clause:
			for_locking_items						{ $$ = $1; }
			| FOR READ ONLY							{ $$ = NIL; }
		;

opt_for_locking_clause:
			for_locking_clause						{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

for_locking_items:
			for_locking_item						{ $$ = list_make1($1); }
			| for_locking_items for_locking_item	{ $$ = lappend($1, $2); }
		;

for_locking_item:
			FOR UPDATE locked_rels_list opt_nowait
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $3;
					n->forUpdate = TRUE;
					n->noWait = $4;
					$$ = (Node *) n;
				}
			| FOR SHARE locked_rels_list opt_nowait
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $3;
					n->forUpdate = FALSE;
					n->noWait = $4;
					$$ = (Node *) n;
				}
		;

locked_rels_list:
			OF name_list							{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NIL; }
		;


values_clause:
			VALUES ctext_row
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->valuesLists = list_make1($2);
					$$ = (Node *) n;
				}
			| values_clause ',' ctext_row
				{
					SelectStmt *n = (SelectStmt *) $1;
					n->valuesLists = lappend(n->valuesLists, $3);
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *	clauses common to all Optimizable Stmts:
 *		from_clause		- allow list of both JOIN expressions and table names
 *		where_clause	- qualifications for joins or restrictions
 *
 *****************************************************************************/

from_clause:
			FROM from_list							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

from_list:
			table_ref								{ $$ = list_make1($1); }
			| from_list ',' table_ref				{ $$ = lappend($1, $3); }
		;

/*
 * table_ref is where an alias clause can be attached.	Note we cannot make
 * alias_clause have an empty production because that causes parse conflicts
 * between table_ref := '(' joined_table ')' alias_clause
 * and joined_table := '(' joined_table ')'.  So, we must have the
 * redundant-looking productions here instead.
 */
table_ref:	relation_expr
				{
					$$ = (Node *) $1;
				}
			| relation_expr alias_clause
				{
					$1->alias = $2;
					$$ = (Node *) $1;
				}
			| func_table
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->funccallnode = $1;
					n->coldeflist = NIL;
					$$ = (Node *) n;
				}
			| func_table alias_clause
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->funccallnode = $1;
					n->alias = $2;
					n->coldeflist = NIL;
					$$ = (Node *) n;
				}
			| func_table AS '(' TableFuncElementList ')'
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->funccallnode = $1;
					n->coldeflist = $4;
					$$ = (Node *) n;
				}
			| func_table AS ColId '(' TableFuncElementList ')'
				{
					RangeFunction *n = makeNode(RangeFunction);
					Alias *a = makeNode(Alias);
					n->funccallnode = $1;
					a->aliasname = $3;
					n->alias = a;
					n->coldeflist = $5;
					$$ = (Node *) n;
				}
			| func_table ColId '(' TableFuncElementList ')'
				{
					RangeFunction *n = makeNode(RangeFunction);
					Alias *a = makeNode(Alias);
					n->funccallnode = $1;
					a->aliasname = $2;
					n->alias = a;
					n->coldeflist = $4;
					$$ = (Node *) n;
				}
			| select_with_parens
				{
					/*
					 * The SQL spec does not permit a subselect
					 * (<derived_table>) without an alias clause,
					 * so we don't either.  This avoids the problem
					 * of needing to invent a unique refname for it.
					 * That could be surmounted if there's sufficient
					 * popular demand, but for now let's just implement
					 * the spec and see if anyone complains.
					 * However, it does seem like a good idea to emit
					 * an error message that's better than "syntax error".
					 */
					if (IsA($1, SelectStmt) &&
						((SelectStmt *) $1)->valuesLists)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("VALUES in FROM must have an alias"),
								 errhint("For example, FROM (VALUES ...) [AS] foo."),
								 scanner_errposition(@1)));
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("subquery in FROM must have an alias"),
								 errhint("For example, FROM (SELECT ...) [AS] foo."),
								 scanner_errposition(@1)));
					$$ = NULL;
				}
			| select_with_parens alias_clause
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->subquery = $1;
					n->alias = $2;
					$$ = (Node *) n;
				}
			| joined_table
				{
					$$ = (Node *) $1;
				}
			| '(' joined_table ')' alias_clause
				{
					$2->alias = $4;
					$$ = (Node *) $2;
				}
		;


/*
 * It may seem silly to separate joined_table from table_ref, but there is
 * method in SQL92's madness: if you don't do it this way you get reduce-
 * reduce conflicts, because it's not clear to the parser generator whether
 * to expect alias_clause after ')' or not.  For the same reason we must
 * treat 'JOIN' and 'join_type JOIN' separately, rather than allowing
 * join_type to expand to empty; if we try it, the parser generator can't
 * figure out when to reduce an empty join_type right after table_ref.
 *
 * Note that a CROSS JOIN is the same as an unqualified
 * INNER JOIN, and an INNER JOIN/ON has the same shape
 * but a qualification expression to limit membership.
 * A NATURAL JOIN implicitly matches column names between
 * tables and the shape is determined by which columns are
 * in common. We'll collect columns during the later transformations.
 */

joined_table:
			'(' joined_table ')'
				{
					$$ = $2;
				}
			| table_ref CROSS JOIN table_ref
				{
					/* CROSS JOIN is same as unqualified inner join */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL;
					n->quals = NULL;
					$$ = n;
				}
			| table_ref join_type JOIN table_ref join_qual
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					if ($5 != NULL && IsA($5, List))
						n->usingClause = (List *) $5; /* USING clause */
					else
						n->quals = $5; /* ON clause */
					$$ = n;
				}
			| table_ref JOIN table_ref join_qual
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $3;
					if ($4 != NULL && IsA($4, List))
						n->usingClause = (List *) $4; /* USING clause */
					else
						n->quals = $4; /* ON clause */
					$$ = n;
				}
			| table_ref NATURAL join_type JOIN table_ref
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $3;
					n->isNatural = TRUE;
					n->larg = $1;
					n->rarg = $5;
					n->usingClause = NIL; /* figure out which columns later... */
					n->quals = NULL; /* fill later */
					$$ = n;
				}
			| table_ref NATURAL JOIN table_ref
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = TRUE;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL; /* figure out which columns later... */
					n->quals = NULL; /* fill later */
					$$ = n;
				}
		;

alias_clause:
			AS ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
					$$->colnames = $4;
				}
			| AS ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
				}
			| ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
					$$->colnames = $3;
				}
			| ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
				}
		;

join_type:	FULL join_outer							{ $$ = JOIN_FULL; }
			| LEFT join_outer						{ $$ = JOIN_LEFT; }
			| RIGHT join_outer						{ $$ = JOIN_RIGHT; }
			| INNER_P								{ $$ = JOIN_INNER; }
		;

/* OUTER is just noise... */
join_outer: OUTER_P									{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* JOIN qualification clauses
 * Possibilities are:
 *	USING ( column list ) allows only unqualified column names,
 *						  which must match between tables.
 *	ON expr allows more general qualifications.
 *
 * We return USING as a List node, while an ON-expr will not be a List.
 */

join_qual:	USING '(' name_list ')'					{ $$ = (Node *) $3; }
			| ON a_expr								{ $$ = $2; }
		;


relation_expr:
			qualified_name
				{
					/* default inheritance */
					$$ = $1;
					$$->inhOpt = INH_DEFAULT;
					$$->alias = NULL;
				}
			| qualified_name '*'
				{
					/* inheritance query */
					$$ = $1;
					$$->inhOpt = INH_YES;
					$$->alias = NULL;
				}
			| ONLY qualified_name
				{
					/* no inheritance */
					$$ = $2;
					$$->inhOpt = INH_NO;
					$$->alias = NULL;
				}
			| ONLY '(' qualified_name ')'
				{
					/* no inheritance, SQL99-style syntax */
					$$ = $3;
					$$->inhOpt = INH_NO;
					$$->alias = NULL;
				}
		;


/*
 * Given "UPDATE foo set set ...", we have to decide without looking any
 * further ahead whether the first "set" is an alias or the UPDATE's SET
 * keyword.  Since "set" is allowed as a column name both interpretations
 * are feasible.  We resolve the shift/reduce conflict by giving the first
 * relation_expr_opt_alias production a higher precedence than the SET token
 * has, causing the parser to prefer to reduce, in effect assuming that the
 * SET is not an alias.
 */
relation_expr_opt_alias: relation_expr					%prec UMINUS
				{
					$$ = $1;
				}
			| relation_expr ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $2;
					$1->alias = alias;
					$$ = $1;
				}
			| relation_expr AS ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $3;
					$1->alias = alias;
					$$ = $1;
				}
		;


func_table: func_expr								{ $$ = $1; }
		;


where_clause:
			WHERE a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* variant for UPDATE and DELETE */
where_or_current_clause: 
			where_clause							{ $$ = $1; }
			| WHERE CURRENT OF name
				{
					CurrentOfExpr *n = makeNode(CurrentOfExpr);
					n->cursor_name = $4;
					$$ = (Node *) n;
				}
		;


TableFuncElementList:
			TableFuncElement
				{
					$$ = list_make1($1);
				}
			| TableFuncElementList ',' TableFuncElement
				{
					$$ = lappend($1, $3);
				}
		;

TableFuncElement:	ColId Typename
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typname = $2;
					n->constraints = NIL;
					n->is_local = true;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *	Type syntax
 *		SQL92 introduces a large amount of type-specific syntax.
 *		Define individual clauses to handle these cases, and use
 *		 the generic case to handle regular type-extensible Postgres syntax.
 *		- thomas 1997-10-10
 *
 *****************************************************************************/

Typename:	SimpleTypename opt_array_bounds
				{
					$$ = $1;
					$$->arrayBounds = $2;
				}
			| SETOF SimpleTypename opt_array_bounds
				{
					$$ = $2;
					$$->arrayBounds = $3;
					$$->setof = TRUE;
				}
			/* SQL standard syntax, currently only one-dimensional */
			| SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger($4));
				}
			| SETOF SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger($5));
					$$->setof = TRUE;
				}
		;

opt_array_bounds:
			opt_array_bounds '[' ']'
					{  $$ = lappend($1, makeInteger(-1)); }
			| opt_array_bounds '[' Iconst ']'
					{  $$ = lappend($1, makeInteger($3)); }
			| /*EMPTY*/
					{  $$ = NIL; }
		;

SimpleTypename:
			GenericType								{ $$ = $1; }
			| Numeric								{ $$ = $1; }
			| Bit									{ $$ = $1; }
			| Character								{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
			| ConstInterval opt_interval
				{
					$$ = $1;
					if ($2 != INTERVAL_FULL_RANGE)
						$$->typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, $2);
				}
			| ConstInterval '(' Iconst ')' opt_interval
				{
					$$ = $1;
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("INTERVAL(%d) precision must not be negative", $3),
								 scanner_errposition(@3)));
					if ($3 > MAX_INTERVAL_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("INTERVAL(%d) precision reduced to maximum allowed, %d",
										$3, MAX_INTERVAL_PRECISION),
								 scanner_errposition(@3)));
						$3 = MAX_INTERVAL_PRECISION;
					}
					$$->typmod = INTERVAL_TYPMOD($3, $5);
				}
			| type_name attrs
				{
					$$ = makeNode(TypeName);
					$$->names = lcons(makeString($1), $2);
					$$->typmod = -1;
					$$->location = @1;
				}
		;

/* We have a separate ConstTypename to allow defaulting fixed-length
 * types such as CHAR() and BIT() to an unspecified length.
 * SQL9x requires that these default to a length of one, but this
 * makes no sense for constructs like CHAR 'hi' and BIT '0101',
 * where there is an obvious better choice to make.
 * Note that ConstInterval is not included here since it must
 * be pushed up higher in the rules to accomodate the postfix
 * options (e.g. INTERVAL '1' YEAR).
 */
ConstTypename:
			GenericType								{ $$ = $1; }
			| Numeric								{ $$ = $1; }
			| ConstBit								{ $$ = $1; }
			| ConstCharacter						{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
		;

GenericType:
			type_name
				{
					$$ = makeTypeName($1);
					$$->location = @1;
				}
		;

/* SQL92 numeric data types
 * Check FLOAT() precision limits assuming IEEE floating types.
 * - thomas 1997-09-18
 * Provide real DECIMAL() and NUMERIC() implementations now - Jan 1998-12-30
 */
Numeric:	INT_P
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| INTEGER
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| SMALLINT
				{
					$$ = SystemTypeName("int2");
					$$->location = @1;
				}
			| BIGINT
				{
					$$ = SystemTypeName("int8");
					$$->location = @1;
				}
			| REAL
				{
					$$ = SystemTypeName("float4");
					$$->location = @1;
				}
			| FLOAT_P opt_float
				{
					$$ = $2;
					$$->location = @1;
				}
			| DOUBLE_P PRECISION
				{
					$$ = SystemTypeName("float8");
					$$->location = @1;
				}
			| DECIMAL_P opt_decimal
				{
					$$ = SystemTypeName("numeric");
					$$->typmod = $2;
				}
			| DEC opt_decimal
				{
					$$ = SystemTypeName("numeric");
					$$->typmod = $2;
				}
			| NUMERIC opt_numeric
				{
					$$ = SystemTypeName("numeric");
					$$->typmod = $2;
				}
			| BOOLEAN_P
				{
					$$ = SystemTypeName("bool");
					$$->location = @1;
				}
		;

opt_float:	'(' Iconst ')'
				{
					/*
					 * Check FLOAT() precision limits assuming IEEE floating
					 * types - thomas 1997-09-18
					 */
					if ($2 < 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be at least 1 bit"),
								 scanner_errposition(@2)));
					else if ($2 <= 24)
						$$ = SystemTypeName("float4");
					else if ($2 <= 53)
						$$ = SystemTypeName("float8");
					else
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be less than 54 bits"),
								 scanner_errposition(@2)));
				}
			| /*EMPTY*/
				{
					$$ = SystemTypeName("float8");
				}
		;

opt_numeric:
			'(' Iconst ',' Iconst ')'
				{
					if ($2 < 1 || $2 > NUMERIC_MAX_PRECISION)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("NUMERIC precision %d must be between 1 and %d",
										$2, NUMERIC_MAX_PRECISION)));
					if ($4 < 0 || $4 > $2)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("NUMERIC scale %d must be between 0 and precision %d",
										$4, $2)));

					$$ = (($2 << 16) | $4) + VARHDRSZ;
				}
			| '(' Iconst ')'
				{
					if ($2 < 1 || $2 > NUMERIC_MAX_PRECISION)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("NUMERIC precision %d must be between 1 and %d",
										$2, NUMERIC_MAX_PRECISION)));

					$$ = ($2 << 16) + VARHDRSZ;
				}
			| /*EMPTY*/
				{
					/* Insert "-1" meaning "no limit" */
					$$ = -1;
				}
		;

opt_decimal:
			'(' Iconst ',' Iconst ')'
				{
					if ($2 < 1 || $2 > NUMERIC_MAX_PRECISION)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("DECIMAL precision %d must be between 1 and %d",
										$2, NUMERIC_MAX_PRECISION)));
					if ($4 < 0 || $4 > $2)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("DECIMAL scale %d must be between 0 and precision %d",
										$4, $2)));

					$$ = (($2 << 16) | $4) + VARHDRSZ;
				}
			| '(' Iconst ')'
				{
					if ($2 < 1 || $2 > NUMERIC_MAX_PRECISION)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("DECIMAL precision %d must be between 1 and %d",
										$2, NUMERIC_MAX_PRECISION)));

					$$ = ($2 << 16) + VARHDRSZ;
				}
			| /*EMPTY*/
				{
					/* Insert "-1" meaning "no limit" */
					$$ = -1;
				}
		;


/*
 * SQL92 bit-field data types
 * The following implements BIT() and BIT VARYING().
 */
Bit:		BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
				}
		;

/* ConstBit is like Bit except "BIT" defaults to unspecified length */
/* See notes for ConstCharacter, which addresses same issue for "CHAR" */
ConstBit:	BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
					$$->typmod = -1;
				}
		;

BitWithLength:
			BIT opt_varying '(' Iconst ')'
				{
					char *typname;

					typname = $2 ? "varbit" : "bit";
					$$ = SystemTypeName(typname);
					if ($4 < 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("length for type %s must be at least 1",
										typname)));
					else if ($4 > (MaxAttrSize * BITS_PER_BYTE))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("length for type %s cannot exceed %d",
										typname, MaxAttrSize * BITS_PER_BYTE)));
					$$->typmod = $4;
				}
		;

BitWithoutLength:
			BIT opt_varying
				{
					/* bit defaults to bit(1), varbit to no limit */
					if ($2)
					{
						$$ = SystemTypeName("varbit");
						$$->typmod = -1;
					}
					else
					{
						$$ = SystemTypeName("bit");
						$$->typmod = 1;
					}
					$$->location = @1;
				}
		;


/*
 * SQL92 character data types
 * The following implements CHAR() and VARCHAR().
 */
Character:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					$$ = $1;
				}
		;

ConstCharacter:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					/* Length was not specified so allow to be unrestricted.
					 * This handles problems with fixed-length (bpchar) strings
					 * which in column definitions must default to a length
					 * of one, but should not be constrained if the length
					 * was not specified.
					 */
					$$ = $1;
					$$->typmod = -1;
				}
		;

CharacterWithLength:  character '(' Iconst ')' opt_charset
				{
					if (($5 != NULL) && (strcmp($5, "sql_text") != 0))
					{
						char *type;

						type = palloc(strlen($1) + 1 + strlen($5) + 1);
						strcpy(type, $1);
						strcat(type, "_");
						strcat(type, $5);
						$1 = type;
					}

					$$ = SystemTypeName($1);

					if ($3 < 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("length for type %s must be at least 1",
										$1)));
					else if ($3 > MaxAttrSize)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("length for type %s cannot exceed %d",
										$1, MaxAttrSize)));

					/* we actually implement these like a varlen, so
					 * the first 4 bytes is the length. (the difference
					 * between these and "text" is that we blank-pad and
					 * truncate where necessary)
					 */
					$$->typmod = VARHDRSZ + $3;
				}
		;

CharacterWithoutLength:	 character opt_charset
				{
					if (($2 != NULL) && (strcmp($2, "sql_text") != 0))
					{
						char *type;

						type = palloc(strlen($1) + 1 + strlen($2) + 1);
						strcpy(type, $1);
						strcat(type, "_");
						strcat(type, $2);
						$1 = type;
					}

					$$ = SystemTypeName($1);

					/* char defaults to char(1), varchar to no limit */
					if (strcmp($1, "bpchar") == 0)
						$$->typmod = VARHDRSZ + 1;
					else
						$$->typmod = -1;
				}
		;

character:	CHARACTER opt_varying
										{ $$ = $2 ? "varchar": "bpchar"; }
			| CHAR_P opt_varying
										{ $$ = $2 ? "varchar": "bpchar"; }
			| VARCHAR
										{ $$ = "varchar"; }
			| NATIONAL CHARACTER opt_varying
										{ $$ = $3 ? "varchar": "bpchar"; }
			| NATIONAL CHAR_P opt_varying
										{ $$ = $3 ? "varchar": "bpchar"; }
			| NCHAR opt_varying
										{ $$ = $2 ? "varchar": "bpchar"; }
		;

opt_varying:
			VARYING									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_charset:
			CHARACTER SET ColId						{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/*
 * SQL92 date/time types
 */
ConstDatetime:
			TIMESTAMP '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					/* XXX the timezone field seems to be unused
					 * - thomas 2001-09-06
					 */
					$$->timezone = $5;
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("TIMESTAMP(%d)%s precision must not be negative",
										$3, ($5 ? " WITH TIME ZONE": ""))));
					if ($3 > MAX_TIMESTAMP_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("TIMESTAMP(%d)%s precision reduced to maximum allowed, %d",
										$3, ($5 ? " WITH TIME ZONE": ""),
										MAX_TIMESTAMP_PRECISION)));
						$3 = MAX_TIMESTAMP_PRECISION;
					}
					$$->typmod = $3;
					$$->location = @1;
				}
			| TIMESTAMP opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					/* XXX the timezone field seems to be unused
					 * - thomas 2001-09-06
					 */
					$$->timezone = $2;
					$$->location = @1;
				}
			| TIME '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("TIME(%d)%s precision must not be negative",
										$3, ($5 ? " WITH TIME ZONE": ""))));
					if ($3 > MAX_TIME_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("TIME(%d)%s precision reduced to maximum allowed, %d",
										$3, ($5 ? " WITH TIME ZONE": ""),
										MAX_TIME_PRECISION)));
						$3 = MAX_TIME_PRECISION;
					}
					$$->typmod = $3;
					$$->location = @1;
				}
			| TIME opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->location = @1;
				}
		;

ConstInterval:
			INTERVAL
				{
					$$ = SystemTypeName("interval");
					$$->location = @1;
				}
		;

opt_timezone:
			WITH_TIME ZONE							{ $$ = TRUE; }
			| WITHOUT TIME ZONE						{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_interval:
			YEAR_P									{ $$ = INTERVAL_MASK(YEAR); }
			| MONTH_P								{ $$ = INTERVAL_MASK(MONTH); }
			| DAY_P									{ $$ = INTERVAL_MASK(DAY); }
			| HOUR_P								{ $$ = INTERVAL_MASK(HOUR); }
			| MINUTE_P								{ $$ = INTERVAL_MASK(MINUTE); }
			| SECOND_P								{ $$ = INTERVAL_MASK(SECOND); }
			| YEAR_P TO MONTH_P
					{ $$ = INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH); }
			| DAY_P TO HOUR_P
					{ $$ = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR); }
			| DAY_P TO MINUTE_P
					{ $$ = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)
						| INTERVAL_MASK(MINUTE); }
			| DAY_P TO SECOND_P
					{ $$ = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)
						| INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND); }
			| HOUR_P TO MINUTE_P
					{ $$ = INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE); }
			| HOUR_P TO SECOND_P
					{ $$ = INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
						| INTERVAL_MASK(SECOND); }
			| MINUTE_P TO SECOND_P
					{ $$ = INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND); }
			| /*EMPTY*/								{ $$ = INTERVAL_FULL_RANGE; }
		;


/*****************************************************************************
 *
 *	expression grammar
 *
 *****************************************************************************/

/*
 * General expressions
 * This is the heart of the expression syntax.
 *
 * We have two expression types: a_expr is the unrestricted kind, and
 * b_expr is a subset that must be used in some places to avoid shift/reduce
 * conflicts.  For example, we can't do BETWEEN as "BETWEEN a_expr AND a_expr"
 * because that use of AND conflicts with AND as a boolean operator.  So,
 * b_expr is used in BETWEEN and we remove boolean keywords from b_expr.
 *
 * Note that '(' a_expr ')' is a b_expr, so an unrestricted expression can
 * always be used by surrounding it with parens.
 *
 * c_expr is all the productions that are common to a_expr and b_expr;
 * it's factored out just to eliminate redundant coding.
 */
a_expr:		c_expr									{ $$ = $1; }
			| a_expr TYPECAST Typename
					{ $$ = makeTypeCast($1, $3, @2); }
			| a_expr AT TIME ZONE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("timezone");
					n->args = list_make2($5, $1);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @2;
					$$ = (Node *) n;
				}
		/*
		 * These operators must be called out explicitly in order to make use
		 * of yacc/bison's automatic operator-precedence handling.  All other
		 * operator names are handled by the generic productions using "Op",
		 * below; and all those operators will have the same precedence.
		 *
		 * If you add more explicitly-known operators, be sure to add them
		 * also to b_expr and to the MathOp list above.
		 */
			| '+' a_expr					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' a_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| a_expr '+' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| a_expr '-' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| a_expr '*' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| a_expr '/' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| a_expr '%' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| a_expr '^' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| a_expr '<' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| a_expr '>' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| a_expr '=' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }

			| a_expr qual_Op a_expr				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op a_expr					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| a_expr qual_Op					%prec POSTFIXOP
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }

			| a_expr AND a_expr
				{ $$ = (Node *) makeA_Expr(AEXPR_AND, NIL, $1, $3, @2); }
			| a_expr OR a_expr
				{ $$ = (Node *) makeA_Expr(AEXPR_OR, NIL, $1, $3, @2); }
			| NOT a_expr
				{ $$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, $2, @1); }

			| a_expr LIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, $3, @2); }
			| a_expr LIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($3, $5);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @4;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, (Node *) n, @2);
				}
			| a_expr NOT LIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, $4, @2); }
			| a_expr NOT LIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($4, $6);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @5;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, (Node *) n, @2);
				}
			| a_expr ILIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~*", $1, $3, @2); }
			| a_expr ILIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($3, $5);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @4;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~*", $1, (Node *) n, @2);
				}
			| a_expr NOT ILIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~*", $1, $4, @2); }
			| a_expr NOT ILIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($4, $6);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @5;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~*", $1, (Node *) n, @2);
				}

			| a_expr SIMILAR TO a_expr				%prec SIMILAR
				{
					A_Const *c = makeNode(A_Const);
					FuncCall *n = makeNode(FuncCall);
					c->val.type = T_Null;
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($4, (Node *) c);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @2;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~", $1, (Node *) n, @2);
				}
			| a_expr SIMILAR TO a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($4, $6);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @5;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~", $1, (Node *) n, @2);
				}
			| a_expr NOT SIMILAR TO a_expr			%prec SIMILAR
				{
					A_Const *c = makeNode(A_Const);
					FuncCall *n = makeNode(FuncCall);
					c->val.type = T_Null;
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($5, (Node *) c);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @5;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~", $1, (Node *) n, @2);
				}
			| a_expr NOT SIMILAR TO a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($5, $7);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @6;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~", $1, (Node *) n, @2);
				}

			/* NullTest clause
			 * Define SQL92-style Null test clause.
			 * Allow two forms described in the standard:
			 *	a IS NULL
			 *	a IS NOT NULL
			 * Allow two SQL extensions
			 *	a ISNULL
			 *	a NOTNULL
			 */
			| a_expr IS NULL_P
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					$$ = (Node *)n;
				}
			| a_expr ISNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					$$ = (Node *)n;
				}
			| a_expr IS NOT NULL_P
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					$$ = (Node *)n;
				}
			| a_expr NOTNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					$$ = (Node *)n;
				}
			| row OVERLAPS row
				{
					$$ = (Node *)makeOverlaps($1, $3, @2);
				}
			| a_expr IS TRUE_P
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_TRUE;
					$$ = (Node *)b;
				}
			| a_expr IS NOT TRUE_P
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_TRUE;
					$$ = (Node *)b;
				}
			| a_expr IS FALSE_P
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_FALSE;
					$$ = (Node *)b;
				}
			| a_expr IS NOT FALSE_P
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_FALSE;
					$$ = (Node *)b;
				}
			| a_expr IS UNKNOWN
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_UNKNOWN;
					$$ = (Node *)b;
				}
			| a_expr IS NOT UNKNOWN
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_UNKNOWN;
					$$ = (Node *)b;
				}
			| a_expr IS DISTINCT FROM a_expr			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| a_expr IS NOT DISTINCT FROM a_expr		%prec IS
				{
					$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL,
									(Node *) makeSimpleA_Expr(AEXPR_DISTINCT,
															  "=", $1, $6, @2),
											 @2);

				}
			| a_expr IS OF '(' type_list ')'			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| a_expr IS NOT OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}
			/*
			 *	Ideally we would not use hard-wired operators below but instead use
			 *	opclasses.  However, mixed data types and other issues make this
			 *	difficult:  http://archives.postgresql.org/pgsql-hackers/2008-08/msg01142.php
			 */			
			| a_expr BETWEEN opt_asymmetric b_expr AND b_expr		%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_AND, NIL,
						(Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $4, @2),
						(Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $6, @2),
											 @2);
				}
			| a_expr NOT BETWEEN opt_asymmetric b_expr AND b_expr	%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_OR, NIL,
						(Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $5, @2),
						(Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $7, @2),
											 @2);
				}
			| a_expr BETWEEN SYMMETRIC b_expr AND b_expr			%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_OR, NIL,
						(Node *) makeA_Expr(AEXPR_AND, NIL,
						    (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $4, @2),
						    (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $6, @2),
											@2),
						(Node *) makeA_Expr(AEXPR_AND, NIL,
						    (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $6, @2),
						    (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $4, @2),
											@2),
											 @2);
				}
			| a_expr NOT BETWEEN SYMMETRIC b_expr AND b_expr		%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_AND, NIL,
						(Node *) makeA_Expr(AEXPR_OR, NIL,
						    (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $5, @2),
						    (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $7, @2),
											@2),
						(Node *) makeA_Expr(AEXPR_OR, NIL,
						    (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $7, @2),
						    (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $5, @2),
											@2),
											 @2);
				}
			| a_expr IN_P in_expr
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($3, SubLink))
					{
						/* generate foo = ANY (subquery) */
						SubLink *n = (SubLink *) $3;
						n->subLinkType = ANY_SUBLINK;
						n->testexpr = $1;
						n->operName = list_make1(makeString("="));
    					n->location = @2;
						$$ = (Node *)n;
					}
					else
					{
						/* generate scalar IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "=", $1, $3, @2);
					}
				}
			| a_expr NOT IN_P in_expr
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($4, SubLink))
					{
						/* generate NOT (foo = ANY (subquery)) */
						/* Make an = ANY node */
						SubLink *n = (SubLink *) $4;
						n->subLinkType = ANY_SUBLINK;
						n->testexpr = $1;
						n->operName = list_make1(makeString("="));
    					n->location = @3;
						/* Stick a NOT on top */
						$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, (Node *) n, @2);
					}
					else
					{
						/* generate scalar NOT IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, $4, @2);
					}
				}
			| a_expr subquery_Op sub_type select_with_parens	%prec Op
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = $3;
					n->testexpr = $1;
					n->operName = $2;
					n->subselect = $4;
					n->location = @3;
					$$ = (Node *)n;
				}
			| a_expr subquery_Op sub_type '(' a_expr ')'		%prec Op
				{
					if ($3 == ANY_SUBLINK)
						$$ = (Node *) makeA_Expr(AEXPR_OP_ANY, $2, $1, $5, @2);
					else
						$$ = (Node *) makeA_Expr(AEXPR_OP_ALL, $2, $1, $5, @2);
				}
			| UNIQUE select_with_parens
				{
					/* Not sure how to get rid of the parentheses
					 * but there are lots of shift/reduce errors without them.
					 *
					 * Should be able to implement this by plopping the entire
					 * select into a node, then transforming the target expressions
					 * from whatever they are into count(*), and testing the
					 * entire result equal to one.
					 * But, will probably implement a separate node in the executor.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNIQUE predicate is not yet implemented"),
							 scanner_errposition(@1)));
				}
		;

/*
 * Restricted expressions
 *
 * b_expr is a subset of the complete expression syntax defined by a_expr.
 *
 * Presently, AND, NOT, IS, and IN are the a_expr keywords that would
 * cause trouble in the places where b_expr is used.  For simplicity, we
 * just eliminate all the boolean-keyword-operator productions from b_expr.
 */
b_expr:		c_expr
				{ $$ = $1; }
			| b_expr TYPECAST Typename
				{ $$ = makeTypeCast($1, $3, @2); }
			| '+' b_expr					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' b_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| b_expr '+' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| b_expr '-' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| b_expr '*' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| b_expr '/' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| b_expr '%' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| b_expr '^' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| b_expr '<' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| b_expr '>' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| b_expr '=' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| b_expr qual_Op b_expr				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op b_expr					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| b_expr qual_Op					%prec POSTFIXOP
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }
			| b_expr IS DISTINCT FROM b_expr		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| b_expr IS NOT DISTINCT FROM b_expr	%prec IS
				{
					$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL,
						NULL, (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $6, @2), @2);
				}
			| b_expr IS OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| b_expr IS NOT OF '(' type_list ')'	%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}

		;

/*
 * Productions that can be used in both a_expr and b_expr.
 *
 * Note: productions that refer recursively to a_expr or b_expr mostly
 * cannot appear here.	However, it's OK to refer to a_exprs that occur
 * inside parentheses, such as function arguments; that cannot introduce
 * ambiguity to the b_expr syntax.
 */
c_expr:		columnref								{ $$ = $1; }
			| func_expr OVER '(' window_spec ')'
				{
					/*
					 * We break out the window function from func_expr
					 * to avoid shift/reduce errors.
					 */
					if (IsA($1, FuncCall))
						((FuncCall *)$1)->over = $4;
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("window OVER clause can only be used "
										"with an aggregate")));

					$$ = (Node *)$1;
				}
			| AexprConst							{ $$ = $1; }
			| PARAM opt_indirection
				{
					ParamRef *p = makeNode(ParamRef);
					p->number = $1;
					p->location = @1;
					if ($2)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = (Node *) p;
						n->indirection = $2;
						$$ = (Node *) n;
					}
					else
						$$ = (Node *) p;
				}
			| '(' a_expr ')' opt_indirection
				{
					if ($4)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = $2;
						n->indirection = $4;
						$$ = (Node *)n;
					}
					else
						$$ = $2;
				}
			| case_expr
				{ $$ = $1; }
			| decode_expr
				{ $$ = $1; }
			| func_expr
				{ $$ = $1; }
			| select_with_parens			%prec UMINUS
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXPR_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					$$ = (Node *)n;
				}
			| EXISTS select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXISTS_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = ARRAY_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY array_expr
				{	$$ = $2;	}
            | TABLE '(' table_value_select_clause ')'
				{	
					TableValueExpr *n = makeNode(TableValueExpr);
					n->subquery = $3;
					n->location = @1;
					$$ = (Node*) n;
				}
			| row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->location = @1;
					$$ = (Node *)r;
				}
		;

scatter_clause:
		/* EMPTY */						{ $$ = NIL; }
		| SCATTER RANDOMLY				{ $$ = list_make1(NULL); }
		| SCATTER BY expr_list 			{ $$ = $3; }
  		;

table_value_select_clause:
		SelectStmt scatter_clause
		{
			SelectStmt	*s	 = (SelectStmt *) $1;
			s->scatterClause = $2;
			$$ = (Node *) s;
		}
  		;

/*
 * Users can write their own inline specification or refer to a
 * specification made after the WHERE clause
 */

window_name: ColId { $$ = $1; }
		;


simple_func: 	func_name '(' ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->agg_filter = NULL;
					n->location = @1;
					n->over = NULL;
					$$ = (Node *)n;
				}
			| func_name '(' expr_list opt_sort_clause ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $3;
                    n->agg_order = $4;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->agg_filter = NULL;
					n->location = @1;
					n->over = NULL;
					$$ = (Node *)n;
				}
			| func_name '(' ALL expr_list opt_sort_clause')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $4;
                    n->agg_order = $5;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->agg_filter = NULL;
					/* Ideally we'd mark the FuncCall node to indicate
					 * "must be an aggregate", but there's no provision
					 * for that in FuncCall at the moment.
					 */
					n->location = @1;
					n->over = NULL;
					$$ = (Node *)n;
				}
			| func_name '(' DISTINCT expr_list opt_sort_clause')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $4;
                    n->agg_order = $5;
					n->agg_star = FALSE;
					n->agg_distinct = TRUE;
					n->agg_filter = NULL;
					n->location = @1;
					n->over = NULL;
					$$ = (Node *)n;
				}
			| func_name '(' '*' ')'
				{
					/*
					 * We consider AGGREGATE(*) to invoke a parameterless
					 * aggregate.  This does the right thing for COUNT(*),
					 * and there are no other aggregates in SQL92 that accept
					 * '*' as parameter.
					 *
					 * The FuncCall node is also marked agg_star = true,
					 * so that later processing can detect what the argument
					 * really was.
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = TRUE;
					n->agg_distinct = FALSE;
					n->agg_filter = NULL;
					n->location = @1;
					n->over = NULL;
					$$ = (Node *)n;
				}
		;

/*
 * func_expr is split out from c_expr just so that we have a classification
 * for "everything that is a function call or looks like one".  This isn't
 * very important, but it saves us having to document which variants are
 * legal in the backwards-compatible functional-index syntax for CREATE INDEX.
 * (Note that many of the special SQL functions wouldn't actually make any
 * sense as functional index entries, but we ignore that consideration here.)
 */
func_expr:	simple_func FILTER '(' WHERE a_expr ')'
				{
				  FuncCall  *f = (FuncCall *) $1;
				  f->agg_filter = $5;
				  $$ = (Node *)f;
				}
			| simple_func
				{ $$ = $1; }
			| CURRENT_DATE
				{
					/*
					 * Translate as "'now'::text::date".
					 *
					 * We cannot use "'now'::date" because coerce_type() will
					 * immediately reduce that to a constant representing
					 * today's date.  We need to delay the conversion until
					 * runtime, else the wrong things will happen when
					 * CURRENT_DATE is used in a column default value or rule.
					 *
					 * This could be simplified if we had a way to generate
					 * an expression tree representing runtime application
					 * of type-input conversion functions.  (As of PG 7.3
					 * that is actually possible, but not clear that we want
					 * to rely on it.)
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");

					d = SystemTypeName("date");

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| CURRENT_TIME
				{
					/*
					 * Translate as "'now'::text::timetz".
					 * See comments for CURRENT_DATE.
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");

					d = SystemTypeName("timetz");

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| CURRENT_TIME '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::timetz(n)".
					 * See comments for CURRENT_DATE.
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");
					d = SystemTypeName("timetz");
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("CURRENT_TIME(%d) precision must not be negative",
										$3)));
					if ($3 > MAX_TIME_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("CURRENT_TIME(%d) precision reduced to maximum allowed, %d",
										$3, MAX_TIME_PRECISION)));
						$3 = MAX_TIME_PRECISION;
					}
					d->typmod = $3;

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| CURRENT_TIMESTAMP
				{
					/*
					 * Translate as "now()", since we have a function that
					 * does exactly what is needed.
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("now");
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| CURRENT_TIMESTAMP '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::timestamptz(n)".
					 * See comments for CURRENT_DATE.
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");

					d = SystemTypeName("timestamptz");
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("CURRENT_TIMESTAMP(%d) precision must not be negative",
										$3)));
					if ($3 > MAX_TIMESTAMP_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("CURRENT_TIMESTAMP(%d) precision reduced to maximum allowed, %d",
										$3, MAX_TIMESTAMP_PRECISION)));
						$3 = MAX_TIMESTAMP_PRECISION;
					}
					d->typmod = $3;

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| LOCALTIME
				{
					/*
					 * Translate as "'now'::text::time".
					 * See comments for CURRENT_DATE.
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");

					d = SystemTypeName("time");

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| LOCALTIME '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::time(n)".
					 * See comments for CURRENT_DATE.
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");
					d = SystemTypeName("time");
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("LOCALTIME(%d) precision must not be negative",
										$3)));
					if ($3 > MAX_TIME_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("LOCALTIME(%d) precision reduced to maximum allowed, %d",
										$3, MAX_TIME_PRECISION)));
						$3 = MAX_TIME_PRECISION;
					}
					d->typmod = $3;

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| LOCALTIMESTAMP
				{
					/*
					 * Translate as "'now'::text::timestamp".
					 * See comments for CURRENT_DATE.
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");

					d = SystemTypeName("timestamp");

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| LOCALTIMESTAMP '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::timestamp(n)".
					 * See comments for CURRENT_DATE.
					 */
					A_Const *s = makeNode(A_Const);
					TypeName *d;

					s->val.type = T_String;
					s->val.val.str = "now";
					s->typname = SystemTypeName("text");

					d = SystemTypeName("timestamp");
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("LOCALTIMESTAMP(%d) precision must not be negative",
										$3)));
					if ($3 > MAX_TIMESTAMP_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("LOCALTIMESTAMP(%d) precision reduced to maximum allowed, %d",
										$3, MAX_TIMESTAMP_PRECISION)));
						$3 = MAX_TIMESTAMP_PRECISION;
					}
					d->typmod = $3;

					$$ = (Node *)makeTypeCast((Node *)s, d, -1);
				}
			| CURRENT_ROLE
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_user");
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| CURRENT_USER
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_user");
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| SESSION_USER
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("session_user");
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| USER
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_user");
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| CURRENT_CATALOG
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_database");
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					/*n->func_variadic = FALSE;*/
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| CURRENT_SCHEMA
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_schema");
					n->args = NIL;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					/*n->func_variadic = FALSE;*/
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| CAST '(' a_expr AS Typename ')'
				{ $$ = makeTypeCast($3, $5, @1); }
			| EXTRACT '(' extract_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("date_part");
					n->args = $3;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| OVERLAY '(' overlay_list ')'
				{
					/* overlay(A PLACING B FROM C FOR D) is converted to
					 * substring(A, 1, C-1) || B || substring(A, C+1, C+D)
					 * overlay(A PLACING B FROM C) is converted to
					 * substring(A, 1, C-1) || B || substring(A, C+1, C+char_length(B))
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("overlay");
					n->args = $3;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| POSITION '(' position_list ')'
				{
					/* position(A in B) is converted to position(B, A) */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("position");
					n->args = $3;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| SUBSTRING '(' substr_list ')'
				{
					/* substring(A from B for C) is converted to
					 * substring(A, B, C) - thomas 2000-11-28
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("substring");
					n->args = $3;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| TREAT '(' a_expr AS Typename ')'
				{
					/* TREAT(expr AS target) converts expr of a particular type to target,
					 * which is defined to be a subtype of the original expression.
					 * In SQL99, this is intended for use with structured UDTs,
					 * but let's make this a generally useful form allowing stronger
					 * coercions than are handled by implicit casting.
					 */
					FuncCall *n = makeNode(FuncCall);
					/* Convert SystemTypeName() to SystemFuncName() even though
					 * at the moment they result in the same thing.
					 */
					n->funcname = SystemFuncName(((Value *)llast($5->names))->val.str);
					n->args = list_make1($3);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| TRIM '(' BOTH trim_list ')'
				{
					/* various trim expressions are defined in SQL92
					 * - thomas 1997-07-19
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("btrim");
					n->args = $4;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| TRIM '(' LEADING trim_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("ltrim");
					n->args = $4;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| TRIM '(' TRAILING trim_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("rtrim");
					n->args = $4;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| TRIM '(' trim_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("btrim");
					n->args = $3;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| CONVERT '(' a_expr USING any_name ')'
				{
					FuncCall *n = makeNode(FuncCall);
					A_Const *c = makeNode(A_Const);

					c->val.type = T_String;
					c->val.val.str = NameListToQuotedString($5);

					n->funcname = SystemFuncName("convert_using");
					n->args = list_make2($3, c);
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| CONVERT '(' expr_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("convert");
					n->args = $3;
                    n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->over = NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NULLIF '(' a_expr ',' a_expr ')'
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NULLIF, "=", $3, $5, @1);
				}
			| COALESCE '(' expr_list ')'
				{
					CoalesceExpr *c = makeNode(CoalesceExpr);
					c->args = $3;
					$$ = (Node *)c;
				}
			| GREATEST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_GREATEST;
					$$ = (Node *)v;
				}
			| LEAST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_LEAST;
					$$ = (Node *)v;
				}
            | GROUPING '(' expr_list ')'
                {
					GroupingFunc *f = makeNode(GroupingFunc);
					f->args = $3;
					$$ = (Node*)f;
				}

			| GROUP_ID '(' ')'
				{
					GroupId *gid = makeNode(GroupId);
					$$ = (Node *)gid;
				}
			| MEDIAN '(' a_expr ')'
				{
					/*
					 * MEDIAN is parsed as an alias to percentile_cont(0.5).
					 * We keep track of original expression to deparse
					 * it later in views, etc.
					 */
					PercentileExpr *n = makeNode(PercentileExpr);
					SortBy		   *sortby;

					n->perctype = UNKNOWNOID;
					n->args = list_make1(makeAConst(makeFloat(pstrdup("0.5")), @1));
					n->perckind = PERC_MEDIAN;
					sortby = makeNode(SortBy);
					sortby->node = $3;
					sortby->sortby_kind = SORTBY_ASC;
					sortby->useOp = NIL;
					n->sortClause = list_make1(sortby);
					n->location = @1;
					$$ = (Node *) n;
				}
			| PERCENTILE_CONT '(' a_expr ')' WITHIN GROUP_P '(' ORDER BY sortby_list ')'
				{
					/*
					 * PERCENTILE_CONT and PERCENTILE_DISC are supported as
					 * grammer for now.  When it comes to catalog support,
					 * we'll be able to remove this grammer.
					 */
					PercentileExpr *n = makeNode(PercentileExpr);
					n->perctype = UNKNOWNOID;
					n->args = list_make1($3);
					n->perckind = PERC_CONT;
					n->sortClause = $10;
					n->location = @1;
					$$ = (Node *) n;
				}
			| PERCENTILE_DISC '(' a_expr ')' WITHIN GROUP_P '(' ORDER BY sortby_list ')'
				{
					PercentileExpr *n = makeNode(PercentileExpr);
					n->perctype = UNKNOWNOID;
					n->args = list_make1($3);
					n->perckind = PERC_DISC;
					n->sortClause = $10;
					n->location = @1;
					$$ = (Node *) n;
				}
		;

/*
 * Supporting nonterminals for expressions.
 */

/* Explicit row production.
 *
 * SQL99 allows an optional ROW keyword, so we can now do single-element rows
 * without conflicting with the parenthesized a_expr production.  Without the
 * ROW keyword, there must be more than one a_expr inside the parens.
 */
row:		ROW '(' expr_list ')'					{ $$ = $3; }
			| ROW '(' ')'							{ $$ = NIL; }
			| '(' expr_list ',' a_expr ')'			{ $$ = lappend($2, $4); }
		;

sub_type:	ANY										{ $$ = ANY_SUBLINK; }
			| SOME									{ $$ = ANY_SUBLINK; }
			| ALL									{ $$ = ALL_SUBLINK; }
		;

all_Op:		Op										{ $$ = $1; }
			| MathOp								{ $$ = $1; }
		;

MathOp:		 '+'									{ $$ = "+"; }
			| '-'									{ $$ = "-"; }
			| '*'									{ $$ = "*"; }
			| '/'									{ $$ = "/"; }
			| '%'									{ $$ = "%"; }
			| '^'									{ $$ = "^"; }
			| '<'									{ $$ = "<"; }
			| '>'									{ $$ = ">"; }
			| '='									{ $$ = "="; }
		;

qual_Op:	Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

qual_all_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

subquery_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
			| LIKE
					{ $$ = list_make1(makeString("~~")); }
			| NOT LIKE
					{ $$ = list_make1(makeString("!~~")); }
			| ILIKE
					{ $$ = list_make1(makeString("~~*")); }
			| NOT ILIKE
					{ $$ = list_make1(makeString("!~~*")); }
/* cannot put SIMILAR TO here, because SIMILAR TO is a hack.
 * the regular expression is preprocessed by a function (similar_escape),
 * and the ~ operator for posix regular expressions is used.
 *        x SIMILAR TO y     ->    x ~ similar_escape(y)
 * this transformation is made on the fly by the parser upwards.
 * however the SubLink structure which handles any/some/all stuff
 * is not ready for such a thing.
 */
			;

expr_list:	a_expr
				{
					$$ = list_make1($1);
				}
			| expr_list ',' a_expr
				{
					$$ = lappend($1, $3);
				}
		;

extract_list:
			extract_arg FROM a_expr
				{
					A_Const *n = makeNode(A_Const);
					n->val.type = T_String;
					n->val.val.str = $1;
					$$ = list_make2((Node *) n, $3);
				}
			| /*EMPTY*/								{ $$ = NIL; }
		;

type_list:  type_list ',' Typename
				{
					$$ = lappend($1, $3);
				}
			| Typename
				{
					$$ = list_make1($1);
				}
		;

array_expr_list: array_expr
				{	$$ = list_make1($1);		}
			| array_expr_list ',' array_expr
				{	$$ = lappend($1, $3);	}
		;

array_expr: '[' expr_list ']'
				{
					ArrayExpr *n = makeNode(ArrayExpr);
					n->elements = $2;
					$$ = (Node *)n;
				}
			| '[' array_expr_list ']'
				{
					ArrayExpr *n = makeNode(ArrayExpr);
					n->elements = $2;
					$$ = (Node *)n;
				}
		;

/* Allow delimited string SCONST in extract_arg as an SQL extension.
 * - thomas 2001-04-12
 */

extract_arg:
			IDENT									{ $$ = $1; }
			| YEAR_P								{ $$ = "year"; }
			| MONTH_P								{ $$ = "month"; }
			| DAY_P									{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }
			| SCONST								{ $$ = $1; }
		;

/* OVERLAY() arguments
 * SQL99 defines the OVERLAY() function:
 * o overlay(text placing text from int for int)
 * o overlay(text placing text from int)
 */
overlay_list:
			a_expr overlay_placing substr_from substr_for
				{
					$$ = list_make4($1, $2, $3, $4);
				}
			| a_expr overlay_placing substr_from
				{
					$$ = list_make3($1, $2, $3);
				}
		;

overlay_placing:
			PLACING a_expr
				{ $$ = $2; }
		;

/* position_list uses b_expr not a_expr to avoid conflict with general IN */

position_list:
			b_expr IN_P b_expr						{ $$ = list_make2($3, $1); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* SUBSTRING() arguments
 * SQL9x defines a specific syntax for arguments to SUBSTRING():
 * o substring(text from int for int)
 * o substring(text from int) get entire string from starting point "int"
 * o substring(text for int) get first "int" characters of string
 * o substring(text from pattern) get entire string matching pattern
 * o substring(text from pattern for escape) same with specified escape char
 * We also want to support generic substring functions which accept
 * the usual generic list of arguments. So we will accept both styles
 * here, and convert the SQL9x style to the generic list for further
 * processing. - thomas 2000-11-28
 */
substr_list:
			a_expr substr_from substr_for
				{
					$$ = list_make3($1, $2, $3);
				}
			| a_expr substr_for substr_from
				{
					/* not legal per SQL99, but might as well allow it */
					$$ = list_make3($1, $3, $2);
				}
			| a_expr substr_from
				{
					$$ = list_make2($1, $2);
				}
			| a_expr substr_for
				{
					/*
					 * Since there are no cases where this syntax allows
					 * a textual FOR value, we forcibly cast the argument
					 * to int4.  The possible matches in pg_proc are
					 * substring(text,int4) and substring(text,text),
					 * and we don't want the parser to choose the latter,
					 * which it is likely to do if the second argument
					 * is unknown or doesn't have an implicit cast to int4.
					 */
					A_Const *n = makeNode(A_Const);
					n->val.type = T_Integer;
					n->val.val.ival = 1;
					$$ = list_make3($1, (Node *) n,
									makeTypeCast($2, SystemTypeName("int4"), -1));
				}
			| expr_list
				{
					$$ = $1;
				}
			| /*EMPTY*/
				{ $$ = NIL; }
		;

substr_from:
			FROM a_expr								{ $$ = $2; }
		;

substr_for: FOR a_expr								{ $$ = $2; }
		;

trim_list:	a_expr FROM expr_list					{ $$ = lappend($3, $1); }
			| FROM expr_list						{ $$ = $2; }
			| expr_list								{ $$ = $1; }
		;

in_expr:	select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subselect = $1;
					/* other fields will be filled later */
					$$ = (Node *)n;
				}
			| '(' expr_list ')'						{ $$ = (Node *)$2; }
		;

/*
 * Define SQL92-style case clause.
 * - Full specification
 *	CASE WHEN a = b THEN c ... ELSE d END
 * - Implicit argument
 *	CASE a WHEN b THEN c ... ELSE d END
 */
case_expr:	CASE case_arg when_clause_list case_default END_P
				{
					CaseExpr *c = makeNode(CaseExpr);
					c->casetype = InvalidOid; /* not analyzed yet */
					c->arg = (Expr *) $2;
					c->args = $3;
					c->defresult = (Expr *) $4;
					$$ = (Node *)c;
				}
		;

when_clause_list:
			/* There must be at least one */
			when_clause								{ $$ = list_make1($1); }
			| when_clause_list when_clause			{ $$ = lappend($1, $2); }
		;

when_clause:
			WHEN when_operand THEN a_expr
				{
					CaseWhen *w = makeNode(CaseWhen);
					w->expr = (Expr *) $2;
					w->result = (Expr *) $4;
					$$ = (Node *) w;
				}
			;
			
when_operand:
			a_expr							{ $$ = $1; }
			| IS NOT DISTINCT FROM a_expr	{ $$ = makeIsNotDistinctFromNode($5,@2); }
		;

case_default:
			ELSE a_expr								{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

case_arg:	a_expr									{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


/*
 * Oracle-compatible DECODE function:
 * DECODE(lhs, rhs, res [, rhs2, res2 ]... [, def_res]): 
 * 		returns resX if lhs = rhsX, or def_res if no match found
 * It is transformed into: 
 *		CASE lhs WHEN IS NOT DISTINCT FROM rhs THEN res
 *				[WHEN IS NOT DISTINCT FROM rhs2 THEN res2] ... 
 *			ELSE def_res END
 */
decode_expr:	
			DECODE '(' a_expr search_result_list decode_default ')'
				{
					CaseExpr *c = makeNode(CaseExpr);
					c->casetype = InvalidOid; /* not analyzed yet */
					c->arg = (Expr *) $3;
					c->args = $4;
					c->defresult = (Expr *) $5;
					$$ = (Node *) c;
				}
		;
			
search_result_list: 
			search_result							{ $$ = list_make1($1); }
			| search_result_list search_result		{ $$ = lappend($1, $2); }
		;

search_result:
			',' a_expr ',' a_expr
				{
					Node *n = makeIsNotDistinctFromNode($2,@2);
					CaseWhen *w = makeNode(CaseWhen);
					w->expr = (Expr *) n;
					w->result = (Expr *) $4;
					$$ = (Node *) w;
				}
		;
				
decode_default: 	
			',' a_expr	 					{ $$ = $2; }
			| /*EMPTY*/						{ $$ = NULL; }
		;


/*
 * columnref starts with relation_name not ColId, so that OLD and NEW
 * references can be accepted.	Note that when there are more than two
 * dotted names, the first name is not actually a relation name...
 */
columnref:	relation_name
				{
					$$ = makeColumnRef($1, NIL, @1);
				}
			| relation_name indirection
				{
					$$ = makeColumnRef($1, $2, @1);
				}
		;

indirection_el:
			'.' attr_name
				{
					$$ = (Node *) makeString($2);
				}
			| '.' '*'
				{
					$$ = (Node *) makeString("*");
				}
			| '[' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = NULL;
					ai->uidx = $2;
					$$ = (Node *) ai;
				}
			| '[' a_expr ':' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
		;

indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;

opt_indirection:
			/*EMPTY*/								{ $$ = NIL; }
			| opt_indirection indirection_el		{ $$ = lappend($1, $2); }
		;

opt_asymmetric: ASYMMETRIC
			| /*EMPTY*/
		;

/*
 * The SQL spec defines "contextually typed value expressions" and
 * "contextually typed row value constructors", which for our purposes
 * are the same as "a_expr" and "row" except that DEFAULT can appear at
 * the top level.
 */

ctext_expr:
			a_expr					{ $$ = (Node *) $1; }
			| DEFAULT				{ $$ = (Node *) makeNode(SetToDefault); }
		;

ctext_expr_list:
			ctext_expr								{ $$ = list_make1($1); }
			| ctext_expr_list ',' ctext_expr		{ $$ = lappend($1, $3); }
		;

/*
 * We should allow ROW '(' ctext_expr_list ')' too, but that seems to require
 * making VALUES a fully reserved word, which will probably break more apps
 * than allowing the noise-word is worth.
 */
ctext_row: '(' ctext_expr_list ')'					{ $$ = $2; }
		;


/*****************************************************************************
 *
 *	target list for SELECT
 *
 *****************************************************************************/

target_list:
			target_el								{ $$ = list_make1($1); }
			| target_list ',' target_el				{ $$ = lappend($1, $3); }
		;

target_el:	a_expr AS ColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $3;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			/*
			 * Postgres supports omitting AS only for column labels that aren't
			 * any known keyword.  There is an ambiguity against postfix
			 * operators: is "a ! b" an infix expression, or a postfix
			 * expression and a column label?  We prefer to resolve this
			 * as an infix expression, which we accomplish by assigning
			 * IDENT a precedence higher than POSTFIXOP.
			 *
			 * In GPDB, we extent this to allow most
			 * unreserved_keywords by also assigning them a
			 * precedence.  There are certain keywords that can't work
			 * without the as: reserved_keywords, the date modifier
			 * suffixes (DAY, MONTH, YEAR, etc) and a few other
			 * obscure cases.
			 */
			| a_expr IDENT
				{
					$$ = makeNode(ResTarget);
					$$->name = $2;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| a_expr ColLabelNoAs
				{
					$$ = makeNode(ResTarget);
					$$->name = $2;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| a_expr
				{
					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| '*'
				{
					ColumnRef *n = makeNode(ColumnRef);
					n->fields = list_make1(makeString("*"));
					n->location = @1;

					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)n;
					$$->location = @1;
				}
			
		;


/*****************************************************************************
 *
 *	Names and constants
 *
 *****************************************************************************/

relation_name:
			SpecialRuleRelation						{ $$ = $1; }
			| ColId									{ $$ = $1; }
		;

qualified_name_list:
			qualified_name							{ $$ = list_make1($1); }
			| qualified_name_list ',' qualified_name { $$ = lappend($1, $3); }
		;

/*
 * The production for a qualified relation name has to exactly match the
 * production for a qualified func_name, because in a FROM clause we cannot
 * tell which we are parsing until we see what comes after it ('(' for a
 * func_name, something else for a relation). Therefore we allow 'indirection'
 * which may contain subscripts, and reject that case in the C code.
 */
qualified_name:
			relation_name
				{
					$$ = makeNode(RangeVar);
					$$->catalogname = NULL;
					$$->schemaname = NULL;
					$$->relname = $1;
					$$->location = @1;
				}
			| relation_name indirection
				{
					check_qualified_name($2);
					$$ = makeNode(RangeVar);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						case 2:
							$$->catalogname = $1;
							$$->schemaname = strVal(linitial($2));
							$$->relname = strVal(lsecond($2));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2)))));
							break;
					}
					$$->location = @1;
				}
		;

name_list:	name
					{ $$ = list_make1(makeString($1)); }
			| name_list ',' name
					{ $$ = lappend($1, makeString($3)); }
		;


name:		ColId									{ $$ = $1; };

database_name:
			ColId									{ $$ = $1; };

access_method:
			ColId									{ $$ = $1; };

attr_name:	ColLabel								{ $$ = $1; };

index_name: ColId									{ $$ = $1; };

file_name:	Sconst									{ $$ = $1; };

/*
 * The production for a qualified func_name has to exactly match the
 * production for a qualified columnref, because we cannot tell which we
 * are parsing until we see what comes after it ('(' for a func_name,
 * anything else for a columnref).  Therefore we allow 'indirection' which
 * may contain subscripts, and reject that case in the C code.  (If we
 * ever implement SQL99-like methods, such syntax may actually become legal!)
 */
func_name:	function_name
					{ $$ = list_make1(makeString($1)); }
			| relation_name indirection
					{ $$ = check_func_name(lcons(makeString($1), $2)); }
		;


/*
 * Constants
 */
AexprConst: Iconst
				{
					A_Const *n = makeNode(A_Const);
					n->val.type = T_Integer;
					n->val.val.ival = $1;
					n->location = @1;                   /*CDB*/
					$$ = (Node *)n;
				}
			| FCONST
				{
					A_Const *n = makeNode(A_Const);
					n->val.type = T_Float;
					n->val.val.str = $1;
					n->location = @1;                   /*CDB*/
					$$ = (Node *)n;
				}
			| Sconst
				{
					A_Const *n = makeNode(A_Const);
					n->val.type = T_String;
					n->val.val.str = $1;
					n->location = @1;                   /*CDB*/
					$$ = (Node *)n;
				}
			| BCONST
				{
					A_Const *n = makeNode(A_Const);
					n->val.type = T_BitString;
					n->val.val.str = $1;
					n->location = @1;                   /*CDB*/
					$$ = (Node *)n;
				}
			| XCONST
				{
					/* This is a bit constant per SQL99:
					 * Without Feature F511, "BIT data type",
					 * a <general literal> shall not be a
					 * <bit string literal> or a <hex string literal>.
					 */
					A_Const *n = makeNode(A_Const);
					n->val.type = T_BitString;
					n->val.val.str = $1;
					n->location = @1;                   /*CDB*/
					$$ = (Node *)n;
				}
			| ConstTypename Sconst
				{
					A_Const *n = makeNode(A_Const);
					n->typname = $1;
					n->val.type = T_String;
					n->val.val.str = $2;
					n->location = @2;                   /*CDB*/
					$$ = (Node *)n;
				}
			| ConstInterval Sconst opt_interval
				{
					A_Const *n = makeNode(A_Const);
					n->typname = $1;
					n->val.type = T_String;
					n->val.val.str = $2;
					n->location = @2;                   /*CDB*/
					/* precision is not specified, but fields may be... */
					if ($3 != INTERVAL_FULL_RANGE)
						n->typname->typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, $3);
					$$ = (Node *)n;
				}
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					A_Const *n = makeNode(A_Const);
					n->typname = $1;
					n->val.type = T_String;
					n->val.val.str = $5;
					n->location = @5;                   /*CDB*/
					/* precision specified, and fields may be... */
					if ($3 < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("INTERVAL(%d) precision must not be negative",
										$3)));
					if ($3 > MAX_INTERVAL_PRECISION)
					{
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("INTERVAL(%d) precision reduced to maximum allowed, %d",
										$3, MAX_INTERVAL_PRECISION)));
						$3 = MAX_INTERVAL_PRECISION;
					}
					n->typname->typmod = INTERVAL_TYPMOD($3, $6);
					$$ = (Node *)n;
				}
			| TRUE_P
				{
					$$ = (Node *)makeBoolAConst(TRUE, @1);
				}
			| FALSE_P
				{
					$$ = (Node *)makeBoolAConst(FALSE, @1);
				}
			| NULL_P
				{
					A_Const *n = makeNode(A_Const);
					n->val.type = T_Null;
					n->location = @1;                   /*CDB*/
					$$ = (Node *)n;
				}
		;

Iconst:		ICONST									{ $$ = $1; };
Sconst:		SCONST									{ $$ = $1; };
RoleId:		ColId									{ $$ = $1; };
QueueId:	ColId									{ $$ = $1; };

SignedIconst: ICONST								{ $$ = $1; }
			| '+' ICONST							{ $$ = + $2; }
			| '-' ICONST							{ $$ = - $2; }
		;

/*
 * Name classification hierarchy.
 *
 * IDENT is the lexeme returned by the lexer for identifiers that match
 * no known keyword.  In most cases, we can accept certain keywords as
 * names, not only IDENTs.	We prefer to accept as many such keywords
 * as possible to minimize the impact of "reserved words" on programmers.
 * So, we divide names into several possible classes.  The classification
 * is chosen in part to make keywords acceptable as names wherever possible.
 */

/* Column identifier --- names that can be column, table, etc names.
 */
ColId:		IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
		;

/* Type identifier --- names that can be type names.
 */
type_name:	IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
		;

/* Function identifier --- names that can be function names.
 */
function_name:
			IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| func_name_keyword						{ $$ = pstrdup($1); }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| func_name_keyword						{ $$ = pstrdup($1); }
			| reserved_keyword						{ $$ = pstrdup($1); }
		;


/*
 * Keyword category lists.  Generally, every keyword present in
 * the Postgres grammar should appear in exactly one of these lists.
 *
 * Put a new keyword into the first list that it can go into without causing
 * shift or reduce conflicts.  The earlier lists define "less reserved"
 * categories of keywords.
 *
 * Make sure that each keyword's category in parser/kwlist.h matches where
 * it is listed here.  (Someday we may be able to generate these lists and
 * kwlist.h's table from a common master list.)
 */

/* "Unreserved" keywords --- available for use as any kind of name.
 */
unreserved_keyword:
			  ABORT_P
			| ABSOLUTE_P
			| ACCESS
			| ACTION
			| ACTIVE
			| ADD_P
			| ADMIN
			| AFTER
			| AGGREGATE
			| ALSO
			| ALTER
			| ASSERTION
			| ASSIGNMENT
			| AT
			| BACKWARD
			| BEFORE
			| BEGIN_P
			| BY
			| CACHE
			| CALLED
			| CASCADE
			| CASCADED
			| CHAIN
			| CHARACTERISTICS
			| CHECKPOINT
			| CLASS
			| CLOSE
			| CLUSTER
			| COMMENT
			| COMMIT
			| COMMITTED
			| CONCURRENTLY
			| CONNECTION
			| CONSTRAINTS
			| CONTAINS
			| CONTENT_P
			| CONTINUE_P
			| CONVERSION_P
			| COPY
			| COST
			| CREATEDB
			| CREATEEXTTABLE
			| CREATEROLE
			| CREATEUSER
			| CSV
			| CURRENT
			| CURSOR
			| CYCLE
			| DATA_P
			| DATABASE
			| DAY_P
			| DEALLOCATE
			| DECLARE
			| DEFAULTS
			| DEFERRED
			| DEFINER
			| DELETE_P
			| DELIMITER
			| DELIMITERS
			| DENY
			| DISABLE_P
			| DOMAIN_P
			| DOUBLE_P
			| DROP
			| DXL
			| EACH
			| ENABLE_P
			| ENCODING
			| ENCRYPTED
			| ENUM_P
			| ERRORS
			| ESCAPE
			| EVERY
			| EXCHANGE
			| EXCLUDING
			| EXCLUSIVE
			| EXECUTE
			| EXPLAIN
			| EXTERNAL
			| FIELDS
			| FILESPACE
			| FILESYSTEM
			| FILL
			| FIRST_P
			| FORCE
			| FORMAT
			| FORMATTER
			| FORWARD
			| FUNCTION
			| GB
			| GLOBAL
			| GRANTED
			| HANDLER
			| HASH
			| HEADER_P
			| HOLD
			| HOST
			| HOUR_P
			| IDENTITY_P
			| IF_P
			| IGNORE_P
			| IMMEDIATE
			| IMMUTABLE
			| IMPLICIT_P
			| INCLUDING
			| INCLUSIVE
			| INCREMENT
			| INDEX
			| INDEXES
			| INHERIT
			| INHERITS
			| INPUT_P
			| INSENSITIVE
			| INSERT
			| INSTEAD
			| INVOKER
			| ISOLATION
			| KB
			| KEEP /* gp */
			| KEY
			| LANCOMPILER
			| LANGUAGE
			| LARGE_P
			| LAST_P
			| LEVEL
			| LIST
			| LISTEN
			| LOAD
			| LOCAL
			| LOCATION
			| LOCK_P
			| LOGIN_P
			| MAPPING
			| MASTER
			| MATCH
			| MAXVALUE
			| MB
			| MERGE
			| MINUTE_P
			| MINVALUE
			| MIRROR
			| MISSING
			| MODE
			| MODIFIES
			| MODIFY
			| MONTH_P
			| MOVE
			| NAME_P
			| NAMES
			| NEWLINE
			| NEXT
			| NO
			| NOCREATEDB
			| NOCREATEEXTTABLE
			| NOCREATEROLE
			| NOCREATEUSER
			| NOINHERIT
			| NOLOGIN_P
			| NOOVERCOMMIT
			| NOSUPERUSER
			| NOTHING
			| NOTIFY
			| NOWAIT
			| NULLS_P
			| OBJECT_P
			| OF
			| OIDS
			| OPERATOR
			| OPTION
			| OPTIONS
			| ORDERED
			| OTHERS
			| OVER
			| OVERCOMMIT
			| OWNED
			| OWNER
			| PARTIAL
			| PARTITIONS
			| PASSWORD
			| PB
			| PERCENT
			| PREPARE
			| PREPARED
			| PRESERVE
			| PRIOR
			| PRIVILEGES
			| PROCEDURAL
			| PROCEDURE
			| PROTOCOL
			| QUEUE
			| QUOTE
			| RANDOMLY /* gp */
			| READ
			| READABLE
			| READS
			| REASSIGN
			| RECHECK
			| RECURSIVE
			| REINDEX
			| REJECT_P /* gp */
			| RELATIVE_P
			| RELEASE
			| RENAME
			| REPEATABLE
			| REPLACE
			| RESET
			| RESOURCE
			| RESTART
			| RESTRICT
			| RETURNS
			| REVOKE
			| ROLE
			| ROLLBACK
			| ROOTPARTITION
			| RULE
			| SAVEPOINT
			| SEARCH
			| SERVER
			| SCHEMA
			| SCROLL
			| SECOND_P
			| SECURITY
			| SEGMENT
			| SEQUENCE
			| SERIALIZABLE
			| SESSION
			| SET
			| SHARE
			| SHOW
			| SIMPLE
			| SPLIT
			| SQL
			| STABLE
			| START
			| STATEMENT
			| STATISTICS
			| STDIN
			| STDOUT
			| STORAGE
			| STRICT_P
			| SUBPARTITION
			| SUBPARTITIONS
			| SUPERUSER_P
			| SYSID
			| SYSTEM_P
			| TABLESPACE
			| TB
			| TEMP
			| TEMPLATE
			| TEMPORARY
			| THRESHOLD
			| TIES
			| TRANSACTION
			| TRIGGER
			| TRUNCATE
			| TRUSTED
			| TYPE_P
			| UNCOMMITTED
			| UNENCRYPTED
			| UNKNOWN
			| UNLISTEN
			| UNTIL
			| UPDATE
			| VACUUM
			| VALID
			| VALIDATION /* gp */
			| VALIDATOR
			| VALUE_P
			| VARYING
			| VERSION_P
			| VIEW
			| VOLATILE
			| WEB /* gp */
			| WITHIN
			| WITHOUT
			| WORK
			| WRAPPER
			| WRITABLE
			| WRITE
			| YEAR_P
			| ZONE
		;

/*
 * ColLabelNoAs is used for SELECT element aliases that don't have the
 * AS keyword.  We always allow IDENT, so anything in double-quotes is
 * also OK.  Beyond that, any keywords listed here can be a column
 * alias even when you omit the AS keyword.
 *
 * We could add some of the reserved_keywords to this list, but I'm
 * reluctant to do so because it might restrict future enhancements to
 * the grammar.
 */

ColLabelNoAs:   keywords_ok_in_alias_no_as   { $$=pstrdup($1); }
				;

keywords_ok_in_alias_no_as: PartitionIdentKeyword
			| TABLESPACE
			| ADD_P
			| ALTER
			| AT
			;

PartitionColId: PartitionIdentKeyword { $$ = pstrdup($1); }
			| IDENT { $$ = pstrdup($1); }
			;

PartitionIdentKeyword: ABORT_P
			| ABSOLUTE_P
			| ACCESS
			| ACTION
			| ACTIVE
			| ADMIN
			| AFTER
			| AGGREGATE
			| ALSO
			| ASSERTION
			| ASSIGNMENT
			| BACKWARD
			| BEFORE
			| BEGIN_P
			| BY
			| CACHE
			| CALLED
			| CASCADE
			| CASCADED
			| CHAIN
			| CHARACTERISTICS
			| CHECKPOINT
			| CLASS
			| CLOSE
			| CLUSTER
			| COMMENT
			| COMMIT
			| COMMITTED
			| CONCURRENTLY
			| CONNECTION
			| CONSTRAINTS
			| CONTAINS
			| CONVERSION_P
			| COPY
			| COST
			| CREATEDB
			| CREATEEXTTABLE
			| CREATEROLE
			| CREATEUSER
			| CSV
			| CURRENT
			| CURSOR
			| CYCLE
			| DATABASE
			| DEALLOCATE
			| DECLARE
			| DEFAULTS
			| DEFERRED
			| DEFINER
			| DELETE_P
			| DELIMITER
			| DELIMITERS
			| DISABLE_P
			| DOMAIN_P
			| DOUBLE_P
			| DROP
			| EACH
			| ENABLE_P
			| ENCODING
			| ENCRYPTED
			| ERRORS
			| ESCAPE
			| EVERY
			| EXCHANGE
			| EXCLUDING
			| EXCLUSIVE
			| EXECUTE
			| EXPLAIN
			| EXTERNAL
			| FIELDS
			| FILL
			| FIRST_P
			| FORCE
			| FORMAT
			| FORMATTER
			| FORWARD
			| FUNCTION
			| GB
			| GLOBAL
			| GRANTED
			| HANDLER
			| HASH
			| HEADER_P
			| HOLD
			| HOST
			| IF_P
			| IMMEDIATE
			| IMMUTABLE
			| IMPLICIT_P
			| INCLUDING
			| INCLUSIVE
			| INCREMENT
			| INDEX
			| INDEXES
			| INHERIT
			| INHERITS
			| INPUT_P
			| INSENSITIVE
			| INSERT
			| INSTEAD
			| INVOKER
			| ISOLATION
			| KB
			| KEY
			| LANCOMPILER
			| LANGUAGE
			| LARGE_P
			| LAST_P
			| LEVEL
			| LIST
			| LISTEN
			| LOAD
			| LOCAL
			| LOCATION
			| LOCK_P
			| LOGIN_P
			| MASTER
			| MATCH
			| MAXVALUE
			| MB
			| MERGE
			| MINVALUE
			| MIRROR
			| MISSING
			| MODE
			| MODIFIES
			| MODIFY
			| MOVE
			| NAMES
			| NEWLINE
			| NEXT
			| NO
			| NOCREATEDB
			| NOCREATEROLE
			| NOCREATEUSER
			| NOINHERIT
			| NOLOGIN_P
			| NOOVERCOMMIT
			| NOSUPERUSER
			| NOTHING
			| NOTIFY
			| NOWAIT
			| OBJECT_P
			| OF
			| OIDS
			| OPERATOR
			| OPTION
			| OTHERS
			| OVERCOMMIT
			| OWNED
			| OWNER
			| PARTIAL
			| PARTITIONS
			| PASSWORD
			| PB
			| PERCENT
			| PREPARE
			| PREPARED
			| PRESERVE
			| PRIOR
			| PRIVILEGES
			| PROCEDURAL
			| PROCEDURE
			| PROTOCOL
			| QUEUE
			| QUOTE
			| READ
			| REASSIGN
			| RECHECK
			| REINDEX
			| RELATIVE_P
			| RELEASE
			| RENAME
			| REPEATABLE
			| REPLACE
			| RESET
			| RESOURCE
			| RESTART
			| RESTRICT
			| RETURNS
			| REVOKE
			| ROLE
			| ROLLBACK
			| RULE
			| SAVEPOINT
			| SCHEMA
			| SCROLL
			| SECURITY
			| SEGMENT
			| SEQUENCE
			| SERIALIZABLE
			| SESSION
			| SET
			| SHARE
			| SHOW
			| SIMPLE
			| SPLIT
			| SQL
			| STABLE
			| START
			| STATEMENT
			| STATISTICS
			| STDIN
			| STDOUT
			| STORAGE
			| STRICT_P
			| SUBPARTITION
			| SUBPARTITIONS
			| SUPERUSER_P
			| SYSID
			| SYSTEM_P
			| TB
			| TEMP
			| TEMPLATE
			| TEMPORARY
			| THRESHOLD
			| TIES
			| TRANSACTION
			| TRIGGER
			| TRUNCATE
			| TRUSTED
			| TYPE_P
			| UNCOMMITTED
			| UNENCRYPTED
			| UNKNOWN
			| UNLISTEN
			| UNTIL
			| UPDATE
			| VACUUM
			| VALID
			| VALIDATOR
			| VIEW
			| VOLATILE
			| WORK
			| WRITE
			| ZONE
			| BIGINT
			| BIT
			| BOOLEAN_P
			| COALESCE
			| CONVERT
			| CUBE
			| DEC
			| DECIMAL_P
			| EXISTS
			| EXTRACT
			| FLOAT_P
			| GREATEST
			| GROUP_ID
			| GROUPING
			| INOUT
			| INT_P
			| INTEGER
			| INTERVAL
			| LEAST
			| NATIONAL
			| NCHAR
			| NONE
			| NULLIF
			| NUMERIC
			| OUT_P
			| OVERLAY
			| POSITION
			| PRECISION
			| REAL
			| ROLLUP
			| ROW
			| SETOF
			| SETS
			| SMALLINT
			| SUBSTRING
			| TIME
			| TIMESTAMP
			| TREAT
			| TRIM
			| VALUES
			| VARCHAR
			| AUTHORIZATION
			| BINARY
			| FREEZE
			| LOG_P
			| OUTER_P
			| VERBOSE
			;

/* Column identifier --- keywords that can be column, table, etc names.
 *
 * Many of these keywords will in fact be recognized as type or function
 * names too; but they have special productions for the purpose, and so
 * can't be treated as "generic" type or function names.
 *
 * The type names appearing here are not usable as function names
 * because they can be followed by '(' in typename productions, which
 * looks too much like a function call for an LR(1) parser.
 * 
 */
col_name_keyword:
			  BIGINT
			| BIT
			| BOOLEAN_P
			| CHAR_P
			| CHARACTER
			| COALESCE
			| CONVERT
			| CUBE
			| DEC
			| DECIMAL_P
			| EXISTS
			| EXTRACT
			| FLOAT_P
			| GREATEST
			| GROUP_ID
			| GROUPING
			| INOUT
			| INT_P
			| INTEGER
			| INTERVAL
			| LEAST
			| MEDIAN
			| NATIONAL
			| NCHAR
			| NONE
			| NULLIF
			| NUMERIC
			| OUT_P
			| OVERLAY
			| PERCENTILE_CONT
			| PERCENTILE_DISC
			| POSITION
			| PRECISION
			| REAL
			| ROLLUP
			| ROW
			| SETOF
			| SETS
			| SMALLINT
			| SUBSTRING
			| TIME
			| TIMESTAMP
			| TREAT
			| TRIM
			| VALUES
			| VARCHAR
		;

/* Function identifier --- keywords that can be function names.
 *
 * Most of these are keywords that are used as operators in expressions;
 * in general such keywords can't be column names because they would be
 * ambiguous with variables, but they are unambiguous as function identifiers.
 *
 * Do not include POSITION, SUBSTRING, etc here since they have explicit
 * productions in a_expr to support the goofy SQL9x argument syntax.
 * - thomas 2000-11-28
 */
func_name_keyword:
			  AUTHORIZATION
			| BINARY
			| CROSS
			| CURRENT_SCHEMA
			| FREEZE
			| FULL
			| ILIKE
			| INNER_P
			| IS
			| ISNULL
			| JOIN
			| LEFT
			| LIKE
			| LOG_P
			| NATURAL
			| NOTNULL
			| OUTER_P
			| OVERLAPS
			| RIGHT
			| SIMILAR
			| VERBOSE
		;

/* Reserved keyword --- these keywords are usable only as a ColLabel.
 *
 * Keywords appear here if they could not be distinguished from variable,
 * type, or function names in some contexts.  Don't put things here unless
 * forced to.
 */
reserved_keyword:
			  ALL
			| ANALYSE
			| ANALYZE
			| AND
			| ANY
			| ARRAY
			| AS
			| ASC
			| ASYMMETRIC
			| BETWEEN
			| BOTH
			| CASE
			| CAST
			| CHECK
			| COLLATE
			| COLUMN
			| CONSTRAINT
			| CREATE
			| CURRENT_CATALOG
			| CURRENT_DATE
			| CURRENT_ROLE
			| CURRENT_TIME
			| CURRENT_TIMESTAMP
			| CURRENT_USER
			| DECODE
			| DEFAULT
			| DEFERRABLE
			| DESC
			| DISTINCT
			| DISTRIBUTED /* gp */
			| DO
			| ELSE
			| END_P
			| EXCEPT
			| EXCLUDE 
			| FALSE_P
			| FETCH
			| FILTER
			| FOLLOWING
			| FOR
			| FOREIGN
			| FROM
			| GRANT
			| GROUP_P
			| HAVING
			| IN_P
			| INITIALLY
			| INTERSECT
			| INTO
			| LEADING
			| LIMIT
			| LOCALTIME
			| LOCALTIMESTAMP
			| NEW
			| NOT
			| NULL_P
			| OFF
			| OFFSET
			| OLD
			| ON
			| ONLY
			| OR
			| ORDER
			| PARTITION
			| PLACING
			| PRECEDING
			| PRIMARY
			| RANGE
			| REFERENCES
			| RETURNING
			| ROWS
			| SCATTER  /* gp */
			| SELECT
			| SESSION_USER
			| SOME
			| SYMMETRIC
			| TABLE
			| THEN
			| TO
			| TRAILING
			| TRUE_P
			| UNBOUNDED
			| UNION
			| UNIQUE
			| USER
			| USING
			| WINDOW
			| WITH
			| WHEN
			| WHERE
		;


SpecialRuleRelation:
			OLD
				{
					if (QueryIsRule)
						$$ = "*OLD*";
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("OLD used in query that is not in a rule")));
				}
			| NEW
				{
					if (QueryIsRule)
						$$ = "*NEW*";
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("NEW used in query that is not in a rule")));
				}
		;

%%

static Node *
makeAddPartitionCreateStmt(Node *n, Node *subSpec)
{
    AlterPartitionCmd *pc_StAttr = (AlterPartitionCmd *)n;
	CreateStmt        *ct        = makeNode(CreateStmt);
    PartitionBy       *pBy       = NULL;

    ct->relation = makeRangeVar(NULL /*catalogname*/, NULL, "fake_partition_name", -1);

    /* in analyze.c, fill in tableelts with a list of inhrelation of
       the partition parent table, and fill in inhrelations with copy
       of rangevar for parent table */

    ct->tableElts    = NIL; /* fill in later */
    ct->inhRelations = NIL; /* fill in later */

    ct->constraints = NIL;

    if (pc_StAttr)
        ct->options = (List *)pc_StAttr->arg1;
    else
        ct->options = NIL;

    ct->oncommit = ONCOMMIT_NOOP;
        
    if (pc_StAttr && pc_StAttr->arg2)
        ct->tablespacename = strVal(pc_StAttr->arg2);
    else
        ct->tablespacename = NULL;

    if (subSpec) /* treat subspec as partition by... */
	{
        pBy = makeNode(PartitionBy); 

        pBy->partSpec = subSpec;
        pBy->partDepth = 0;
		pBy->partQuiet = PART_VERBO_NODISTRO;
        pBy->location  = -1;
        pBy->partDefault = NULL;
        pBy->parentRel = copyObject(ct->relation);
    }

    ct->distributedBy = NULL;
    ct->partitionBy = (Node *)pBy;
    ct->oidInfo.relOid = 0;
    ct->oidInfo.comptypeOid = 0;
    ct->oidInfo.toastOid = 0;
    ct->oidInfo.toastIndexOid = 0;
    ct->oidInfo.toastComptypeOid = 0;
    ct->relKind = RELKIND_RELATION;
    ct->policy = 0;
    ct->postCreate = NULL;

    return (Node *)ct;
}

static Node *
makeColumnRef(char *colname, List *indirection, int location)
{
	/*
	 * Generate a ColumnRef node, with an A_Indirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the ColumnRef node.
	 */
	ColumnRef  *c = makeNode(ColumnRef);
	int		nfields = 0;
	ListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Indices))
		{
			A_Indirection *i = makeNode(A_Indirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to A_Indirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = indirection;
			}
			else
			{
				/* got to split the list in two */
				i->indirection = list_copy_tail(indirection, nfields);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (Node *) c;
			return (Node *) i;
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (Node *) c;
}

static Node *
makeTypeCast(Node *arg, TypeName *typname, int location)
{
	/*
	 * Simply generate a TypeCast node.
	 *
	 * Earlier we would determine whether an A_Const would
	 * be acceptable, however Domains require coerce_type()
	 * to process them -- applying constraints as required.
	 */
	TypeCast *n = makeNode(TypeCast);
	n->arg = arg;
	n->typname = typname;
	n->location = location;
	return (Node *) n;
}

static Node *
makeStringConst(char *str, TypeName *typname, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = str;
	n->typname = typname;
	n->location = location;

	return (Node *)n;
}

static Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);
	n->val.type = T_Integer;
	n->val.val.ival = val;
	n->location = location;
	n->typname = SystemTypeName("int4");

	return (Node *)n;
}

static Node *
makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Float;
	n->val.val.str = str;
	n->location = location;
	n->typname = SystemTypeName("float8");

	return (Node *)n;
}

static Node *
makeAConst(Value *v, int location)
{
	Node *n;

	switch (v->type)
	{
		case T_Float:
			n = makeFloatConst(v->val.str, location);
			break;

		case T_Integer:
			n = makeIntConst(v->val.ival, location);
			break;

		case T_String:
		default:
			n = makeStringConst(v->val.str, NULL, location);
			break;
	}

	return n;
}

/* makeBoolAConst()
 * Create an A_Const node and initialize to a boolean constant.
 */
static A_Const *
makeBoolAConst(bool state, int location)
{
	A_Const *n = makeNode(A_Const);
	n->val.type = T_String;
	n->val.val.str = (state? "t": "f");
	n->typname = SystemTypeName("bool");
	n->location = location;
	return n;
}

/* makeOverlaps()
 * Create and populate a FuncCall node to support the OVERLAPS operator.
 */
static FuncCall *
makeOverlaps(List *largs, List *rargs, int location)
{
	FuncCall *n = makeNode(FuncCall);

	n->funcname = SystemFuncName("overlaps");
	if (list_length(largs) == 1)
		largs = lappend(largs, largs);
	else if (list_length(largs) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("wrong number of parameters on left side of OVERLAPS expression"),
				 scanner_errposition(location)));
	if (list_length(rargs) == 1)
		rargs = lappend(rargs, rargs);
	else if (list_length(rargs) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("wrong number of parameters on right side of OVERLAPS expression"),
				 scanner_errposition(location)));
	n->args = list_concat(largs, rargs);
    n->agg_order = NIL;
	n->agg_star = FALSE;
	n->agg_distinct = FALSE;
	n->over = NULL;
	n->location = location;
	return n;
}

/* check_qualified_name --- check the result of qualified_name production
 *
 * It's easiest to let the grammar production for qualified_name allow
 * subscripts and '*', which we then must reject here.
 */
static void
check_qualified_name(List *names)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			yyerror("syntax error");
		else if (strcmp(strVal(lfirst(i)), "*") == 0)
			yyerror("syntax error");
	}
}

/* check_func_name --- check the result of func_name production
 *
 * It's easiest to let the grammar production for func_name allow subscripts
 * and '*', which we then must reject here.
 */
static List *
check_func_name(List *names)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			yyerror("syntax error");
		else if (strcmp(strVal(lfirst(i)), "*") == 0)
			yyerror("syntax error");
	}
	return names;
}

/* extractArgTypes()
 * Given a list of FunctionParameter nodes, extract a list of just the
 * argument types (TypeNames) for input parameters only.  This is what
 * is needed to look up an existing function, which is what is wanted by
 * the productions that use this call.
 */
static List *
extractArgTypes(List *parameters)
{
	List	   *result = NIL;
	ListCell   *i;

	foreach(i, parameters)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(i);

		/* keep if IN or INOUT */
		if (p->mode != FUNC_PARAM_OUT && p->mode != FUNC_PARAM_TABLE)
			result = lappend(result, p->argType);
	}
	return result;
}

/* findLeftmostSelect()
 * Find the leftmost component SelectStmt in a set-operation parsetree.
 */
static SelectStmt *
findLeftmostSelect(SelectStmt *node)
{
	while (node && node->op != SETOP_NONE)
		node = node->larg;
	Assert(node && IsA(node, SelectStmt) && node->larg == NULL);
	return node;
}

/* insertSelectOptions()
 * Insert ORDER BY, etc into an already-constructed SelectStmt.
 *
 * This routine is just to avoid duplicating code in SelectStmt productions.
 */
static void
insertSelectOptions(SelectStmt *stmt,
					List *sortClause, List *lockingClause,
					Node *limitOffset, Node *limitCount,
					WithClause *withClause)
{
	Assert(IsA(stmt, SelectStmt));

	/*
	 * Tests here are to reject constructs like
	 *	(SELECT foo ORDER BY bar) ORDER BY baz
	 */
	if (sortClause)
	{
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error at or near \"ORDER BY\""),
					 scanner_errposition(exprLocation((Node *) sortClause))));
		stmt->sortClause = sortClause;
	}
	/* We can handle multiple locking clauses, though */
	stmt->lockingClause = list_concat(stmt->lockingClause, lockingClause);
	if (limitOffset)
	{
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error at or near \"OFFSET\""),
					 scanner_errposition(exprLocation(limitOffset))));
		stmt->limitOffset = limitOffset;
	}
	if (limitCount)
	{
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error at or near \"LIMIT\""),
					 scanner_errposition(exprLocation(limitCount))));
		stmt->limitCount = limitCount;
	}
	if (withClause)
	{
		if (stmt->withClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error at or near \"WITH\""),
					 scanner_errposition(exprLocation((Node *)withClause))));
		stmt->withClause = (WithClause *)withClause;
	}

}

static Node *
makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg)
{
	SelectStmt *n = makeNode(SelectStmt);

	n->op = op;
	n->all = all;
	n->larg = (SelectStmt *) larg;
	n->rarg = (SelectStmt *) rarg;
	return (Node *) n;
}

/* SystemFuncName()
 * Build a properly-qualified reference to a built-in function.
 */
List *
SystemFuncName(char *name)
{
	return list_make2(makeString("pg_catalog"), makeString(name));
}

/* SystemTypeName()
 * Build a properly-qualified reference to a built-in type.
 *
 * typmod is defaulted, but may be changed afterwards by caller.
 * Likewise for the location.
 */
TypeName *
SystemTypeName(char *name)
{
	TypeName   *n = makeNode(TypeName);

	n->names = list_make2(makeString("pg_catalog"), makeString(name));
	n->typmod = -1;
	n->location = -1;
	return n;
}

/* parser_init()
 * Initialize to parse one query string
 */
void
parser_init(void)
{
	QueryIsRule = FALSE;
}

/* exprIsNullConstant()
 * Test whether an a_expr is a plain NULL constant or not.
 */
bool
exprIsNullConstant(Node *arg)
{
	if (arg && IsA(arg, A_Const))
	{
		A_Const *con = (A_Const *) arg;

		if (con->val.type == T_Null &&
			con->typname == NULL)
			return TRUE;
	}
	return FALSE;
}

/* doNegate()
 * Handle negation of a numeric constant.
 *
 * Formerly, we did this here because the optimizer couldn't cope with
 * indexquals that looked like "var = -4" --- it wants "var = const"
 * and a unary minus operator applied to a constant didn't qualify.
 * As of Postgres 7.0, that problem doesn't exist anymore because there
 * is a constant-subexpression simplifier in the optimizer.  However,
 * there's still a good reason for doing this here, which is that we can
 * postpone committing to a particular internal representation for simple
 * negative constants.	It's better to leave "-123.456" in string form
 * until we know what the desired type is.
 */
static Node *
doNegate(Node *n, int location)
{
	if (IsA(n, A_Const))
	{
		A_Const *con = (A_Const *)n;

		/* report the constant's location as that of the '-' sign */
		con->location = location;

		if (con->val.type == T_Integer)
		{
			con->val.val.ival = -con->val.val.ival;
			return n;
		}
		if (con->val.type == T_Float)
		{
			doNegateFloat(&con->val);
			return n;
		}
	}

	return (Node *) makeSimpleA_Expr(AEXPR_OP, "-", NULL, n, location);
}

static void
doNegateFloat(Value *v)
{
	char   *oldval = v->val.str;

	Assert(IsA(v, Float));
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval+1;	/* just strip the '-' */
	else
	{
		char   *newval = (char *) palloc(strlen(oldval) + 2);

		*newval = '-';
		strcpy(newval+1, oldval);
		v->val.str = newval;
	}
}

/*
 * Merge the input and output parameters of a table function.
 */
static List *
mergeTableFuncParameters(List *func_args, List *columns)
{
	ListCell   *lc;

	/* Explicit OUT and INOUT parameters shouldn't be used in this syntax */
	foreach(lc, func_args)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(lc);

		switch (p->mode)
		{
			/* Input modes */
			case FUNC_PARAM_IN:
			case FUNC_PARAM_VARIADIC:
				break;  

			/* Output modes */
			case FUNC_PARAM_TABLE:
				Insist(false);  /* not feasible */
				break;
			case FUNC_PARAM_OUT:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("OUT arguments aren't allowed in TABLE functions")));
				break;
			case FUNC_PARAM_INOUT:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("INOUT arguments aren't allowed in TABLE functions")));
				break;
		}
	}

	return list_concat(func_args, columns);
}

/*
 * Determine return type of a TABLE function.  A single result column
 * returns setof that column's type; otherwise return setof record.
 */
static TypeName *
TableFuncTypeName(List *columns)
{
	TypeName *result;

	if (list_length(columns) == 1)
	{
		FunctionParameter *p = (FunctionParameter *) linitial(columns);

		result = (TypeName *) copyObject(p->argType);
	}
	else
		result = SystemTypeName("record");

	result->setof = true;

	return result;
}

static void 
setWindowExclude(WindowFrame *wframe, WindowExclusion exclude)
{
	switch (exclude)
	{
		case WINDOW_EXCLUSION_NULL:
			wframe->exclude = exclude;
			return;

		case WINDOW_EXCLUSION_CUR_ROW:	/* exclude current row */
		case WINDOW_EXCLUSION_GROUP:	/* exclude rows matching us */
		case WINDOW_EXCLUSION_TIES:		/* exclude rows matching us, and current row */
		case WINDOW_EXCLUSION_NO_OTHERS: /* don't exclude */

			/*
			 * because the syntax has historically existed without doing anything
			 * we have chosen to add a guc to allow simply ignoring the exclude clause
			 * rather than raising an error.
			 */
			if (gp_ignore_window_exclude)
			{
				wframe->exclude = WINDOW_EXCLUSION_NULL;
				return;
			}

			/* MPP-13628 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("window EXCLUDE clause not yet implemented")));
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid window EXCLUDE clause")));
			break;
	}
}

/*
 * Create the IS_NOT_DISTINCT_FROM expression node
 *     used by CASE x WHEN IS NOT DISTINCT FROM and DECODE()
 */
static Node*
makeIsNotDistinctFromNode(Node *expr, int position)
{
	Node *n = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL,
								(Node *) makeSimpleA_Expr(AEXPR_DISTINCT, 
	 													"=", NULL, expr, position), position);
	return n;
}

/*
 * Must undefine base_yylex before including scan.c, since we want it
 * to create the function base_yylex not filtered_base_yylex.
 */
#undef base_yylex

#include "scan.c"
