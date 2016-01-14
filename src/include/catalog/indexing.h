/*-------------------------------------------------------------------------
 *
 * indexing.h
 *	  This file provides some definitions to support indexing
 *	  on system catalogs
 *
 *
 * Copyright (c) 2007-2010, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/catalog/indexing.h,v 1.95 2006/07/13 17:47:01 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEXING_H
#define INDEXING_H

#include "access/htup.h"
#include "utils/rel.h"

/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
typedef struct ResultRelInfo *CatalogIndexState;

/*
 * indexing.c prototypes
 */
extern CatalogIndexState CatalogOpenIndexes(Relation heapRel);
extern void CatalogCloseIndexes(CatalogIndexState indstate);
extern void CatalogIndexInsert(CatalogIndexState indstate,
				   HeapTuple heapTuple);
extern void CatalogUpdateIndexes(Relation heapRel, HeapTuple heapTuple);


/*
 * These macros are just to keep the C compiler from spitting up on the
 * upcoming commands for genbki.sh.
 */
#define DECLARE_INDEX(name,oid,decl) extern int no_such_variable
#define DECLARE_UNIQUE_INDEX(name,oid,decl) extern int no_such_variable
#define BUILD_INDICES


/*
 * What follows are lines processed by genbki.sh to create the statements
 * the bootstrap parser will turn into DefineIndex commands.
 *
 * The keyword is DECLARE_INDEX or DECLARE_UNIQUE_INDEX.  The first two
 * arguments are the index name and OID, the rest is much like a standard
 * 'create index' SQL command.
 *
 * For each index, we also provide a #define for its OID.  References to
 * the index in the C code should always use these #defines, not the actual
 * index name (much less the numeric OID).
 */

DECLARE_UNIQUE_INDEX(pg_am_name_index, 2651, on pg_am using btree(amname name_ops));
#define AmNameIndexId  2651
DECLARE_UNIQUE_INDEX(pg_am_oid_index, 2652, on pg_am using btree(oid oid_ops));
#define AmOidIndexId  2652

DECLARE_UNIQUE_INDEX(pg_amop_opc_strat_index, 2653, on pg_amop using btree(amopclaid oid_ops, amopsubtype oid_ops, amopstrategy int2_ops));
#define AccessMethodStrategyIndexId  2653
DECLARE_UNIQUE_INDEX(pg_amop_opr_opc_index, 2654, on pg_amop using btree(amopopr oid_ops, amopclaid oid_ops));
#define AccessMethodOperatorIndexId  2654

DECLARE_UNIQUE_INDEX(pg_amproc_opc_proc_index, 2655, on pg_amproc using btree(amopclaid oid_ops, amprocsubtype oid_ops, amprocnum int2_ops));
#define AccessMethodProcedureIndexId  2655

DECLARE_UNIQUE_INDEX(pg_attrdef_adrelid_adnum_index, 2656, on pg_attrdef using btree(adrelid oid_ops, adnum int2_ops));
#define AttrDefaultIndexId	2656
DECLARE_UNIQUE_INDEX(pg_attrdef_oid_index, 2657, on pg_attrdef using btree(oid oid_ops));
#define AttrDefaultOidIndexId  2657

DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnam_index, 2658, on pg_attribute using btree(attrelid oid_ops, attname name_ops));
#define AttributeRelidNameIndexId  2658
DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnum_index, 2659, on pg_attribute using btree(attrelid oid_ops, attnum int2_ops));
#define AttributeRelidNumIndexId  2659

DECLARE_UNIQUE_INDEX(pg_auth_members_role_member_index, 2694, on pg_auth_members using btree(roleid oid_ops, member oid_ops));
#define AuthMemRoleMemIndexId	2694
DECLARE_UNIQUE_INDEX(pg_auth_members_member_role_index, 2695, on pg_auth_members using btree(member oid_ops, roleid oid_ops));
#define AuthMemMemRoleIndexId	2695

DECLARE_UNIQUE_INDEX(pg_autovacuum_vacrelid_index, 1250, on pg_autovacuum using btree(vacrelid oid_ops));
#define AutovacuumRelidIndexId	1250

DECLARE_UNIQUE_INDEX(pg_cast_oid_index, 2660, on pg_cast using btree(oid oid_ops));
#define CastOidIndexId	2660
DECLARE_UNIQUE_INDEX(pg_cast_source_target_index, 2661, on pg_cast using btree(castsource oid_ops, casttarget oid_ops));
#define CastSourceTargetIndexId  2661

DECLARE_UNIQUE_INDEX(pg_class_oid_index, 2662, on pg_class using btree(oid oid_ops));
#define ClassOidIndexId  2662
DECLARE_UNIQUE_INDEX(pg_class_relname_nsp_index, 2663, on pg_class using btree(relname name_ops, relnamespace oid_ops));
#define ClassNameNspIndexId  2663

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_constraint_conname_nsp_index, 2664, on pg_constraint using btree(conname name_ops, connamespace oid_ops));
#define ConstraintNameNspIndexId  2664
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_constraint_conrelid_index, 2665, on pg_constraint using btree(conrelid oid_ops));
#define ConstraintRelidIndexId	2665
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_constraint_contypid_index, 2666, on pg_constraint using btree(contypid oid_ops));
#define ConstraintTypidIndexId	2666
DECLARE_UNIQUE_INDEX(pg_constraint_oid_index, 2667, on pg_constraint using btree(oid oid_ops));
#define ConstraintOidIndexId  2667

DECLARE_UNIQUE_INDEX(pg_conversion_default_index, 2668, on pg_conversion using btree(connamespace oid_ops, conforencoding int4_ops, contoencoding int4_ops, oid oid_ops));
#define ConversionDefaultIndexId  2668
DECLARE_UNIQUE_INDEX(pg_conversion_name_nsp_index, 2669, on pg_conversion using btree(conname name_ops, connamespace oid_ops));
#define ConversionNameNspIndexId  2669
DECLARE_UNIQUE_INDEX(pg_conversion_oid_index, 2670, on pg_conversion using btree(oid oid_ops));
#define ConversionOidIndexId  2670

DECLARE_UNIQUE_INDEX(pg_database_datname_index, 2671, on pg_database using btree(datname name_ops));
#define DatabaseNameIndexId  2671
DECLARE_UNIQUE_INDEX(pg_database_oid_index, 2672, on pg_database using btree(oid oid_ops));
#define DatabaseOidIndexId	2672

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_depend_depender_index, 2673, on pg_depend using btree(classid oid_ops, objid oid_ops, objsubid int4_ops));
#define DependDependerIndexId  2673
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_depend_reference_index, 2674, on pg_depend using btree(refclassid oid_ops, refobjid oid_ops, refobjsubid int4_ops));
#define DependReferenceIndexId	2674

DECLARE_UNIQUE_INDEX(pg_description_o_c_o_index, 2675, on pg_description using btree(objoid oid_ops, classoid oid_ops, objsubid int4_ops));
#define DescriptionObjIndexId  2675
DECLARE_UNIQUE_INDEX(pg_shdescription_o_c_index, 2397, on pg_shdescription using btree(objoid oid_ops, classoid oid_ops));
#define SharedDescriptionObjIndexId 2397

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_index_indrelid_index, 2678, on pg_index using btree(indrelid oid_ops));
#define IndexIndrelidIndexId  2678
DECLARE_UNIQUE_INDEX(pg_index_indexrelid_index, 2679, on pg_index using btree(indexrelid oid_ops));
#define IndexRelidIndexId  2679

DECLARE_UNIQUE_INDEX(pg_inherits_relid_seqno_index, 2680, on pg_inherits using btree(inhrelid oid_ops, inhseqno int4_ops));
#define InheritsRelidSeqnoIndexId  2680

DECLARE_UNIQUE_INDEX(pg_opclass_am_name_nsp_index, 2686, on pg_opclass using btree(opcamid oid_ops, opcname name_ops, opcnamespace oid_ops));
#define OpclassAmNameNspIndexId  2686
DECLARE_UNIQUE_INDEX(pg_opclass_oid_index, 2687, on pg_opclass using btree(oid oid_ops));
#define OpclassOidIndexId  2687

DECLARE_UNIQUE_INDEX(pg_operator_oid_index, 2688, on pg_operator using btree(oid oid_ops));
#define OperatorOidIndexId	2688
DECLARE_UNIQUE_INDEX(pg_operator_oprname_l_r_n_index, 2689, on pg_operator using btree(oprname name_ops, oprleft oid_ops, oprright oid_ops, oprnamespace oid_ops));
#define OperatorNameNspIndexId	2689

DECLARE_UNIQUE_INDEX(pg_pltemplate_name_index, 1137, on pg_pltemplate using btree(tmplname name_ops));
#define PLTemplateNameIndexId  1137

DECLARE_UNIQUE_INDEX(pg_proc_oid_index, 2690, on pg_proc using btree(oid oid_ops));
#define ProcedureOidIndexId  2690
DECLARE_UNIQUE_INDEX(pg_proc_proname_args_nsp_index, 2691, on pg_proc using btree(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops));
#define ProcedureNameArgsNspIndexId  2691

DECLARE_UNIQUE_INDEX(pg_rewrite_oid_index, 2692, on pg_rewrite using btree(oid oid_ops));
#define RewriteOidIndexId  2692
DECLARE_UNIQUE_INDEX(pg_rewrite_rel_rulename_index, 2693, on pg_rewrite using btree(ev_class oid_ops, rulename name_ops));
#define RewriteRelRulenameIndexId  2693

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_shdepend_depender_index, 1232, on pg_shdepend using btree(dbid oid_ops, classid oid_ops, objid oid_ops));
#define SharedDependDependerIndexId		1232
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_shdepend_reference_index, 1233, on pg_shdepend using btree(refclassid oid_ops, refobjid oid_ops));
#define SharedDependReferenceIndexId	1233

DECLARE_UNIQUE_INDEX(pg_statistic_relid_att_index, 2696, on pg_statistic using btree(starelid oid_ops, staattnum int2_ops));
#define StatisticRelidAttnumIndexId  2696

DECLARE_UNIQUE_INDEX(pg_filespace_oid_index, 2858, on pg_filespace using btree(oid oid_ops));
#define FilespaceOidIndexId  2858
DECLARE_UNIQUE_INDEX(pg_filespace_fsname_index, 2859, on pg_filespace using btree(fsname name_ops));
#define FilespaceNameIndexId  2859

DECLARE_UNIQUE_INDEX(pg_tablespace_oid_index, 2697, on pg_tablespace using btree(oid oid_ops));
#define TablespaceOidIndexId  2697
DECLARE_UNIQUE_INDEX(pg_tablespace_spcname_index, 2698, on pg_tablespace using btree(spcname name_ops));
#define TablespaceNameIndexId  2698

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_trigger_tgconstrname_index, 2699, on pg_trigger using btree(tgconstrname name_ops));
#define TriggerConstrNameIndexId  2699
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_trigger_tgconstrrelid_index, 2700, on pg_trigger using btree(tgconstrrelid oid_ops));
#define TriggerConstrRelidIndexId  2700
DECLARE_UNIQUE_INDEX(pg_trigger_tgrelid_tgname_index, 2701, on pg_trigger using btree(tgrelid oid_ops, tgname name_ops));
#define TriggerRelidNameIndexId  2701
DECLARE_UNIQUE_INDEX(pg_trigger_oid_index, 2702, on pg_trigger using btree(oid oid_ops));
#define TriggerOidIndexId  2702

DECLARE_UNIQUE_INDEX(pg_type_oid_index, 2703, on pg_type using btree(oid oid_ops));
#define TypeOidIndexId	2703
DECLARE_UNIQUE_INDEX(pg_type_typname_nsp_index, 2704, on pg_type using btree(typname name_ops, typnamespace oid_ops));
#define TypeNameNspIndexId	2704

DECLARE_UNIQUE_INDEX(gp_policy_localoid_index, 6103, on gp_distribution_policy using btree(localoid oid_ops));
#define GpPolicyLocalOidIndexId  6103

DECLARE_UNIQUE_INDEX(pg_appendonly_relid_index, 5007, on pg_appendonly using btree(relid oid_ops));
#define AppendOnlyRelidIndexId  5007

DECLARE_UNIQUE_INDEX(pg_appendonly_alter_column_relid_index, 5031, on pg_appendonly_alter_column using btree(relid oid_ops, changenum int4_ops));
#define AppendOnlyAlterColumnRelidIndexId  5031

DECLARE_UNIQUE_INDEX(gp_relfile_node_index, 5095, on gp_relfile_node using btree(relfilenode_oid oid_ops, segment_file_num int4_ops));
#define GpRelfileNodeOidIndexId  5095

/* MPP-6929: metadata tracking */
DECLARE_INDEX(pg_statlastop_classid_objid_index, 6053, on pg_stat_last_operation using btree(classid oid_ops, objid oid_ops));
#define StatLastOpClassidObjidIndexId  6053

DECLARE_UNIQUE_INDEX(pg_statlastop_classid_objid_staactionname_index, 6054, on pg_stat_last_operation using btree(classid oid_ops, objid oid_ops, staactionname name_ops));
#define StatLastOpClassidObjidStaactionnameIndexId  6054

/* Note: no dbid */
DECLARE_INDEX(pg_statlastshop_classid_objid_index, 6057, on pg_stat_last_shoperation using btree(classid oid_ops, objid oid_ops));
#define StatLastShOpClassidObjidIndexId  6057

DECLARE_UNIQUE_INDEX(pg_statlastshop_classid_objid_staactionname_index, 6058, on pg_stat_last_shoperation using btree(classid oid_ops, objid oid_ops, staactionname name_ops));
#define StatLastShOpClassidObjidStaactionnameIndexId  6058

DECLARE_UNIQUE_INDEX(pg_foreign_data_wrapper_oid_index, 3306, on pg_foreign_data_wrapper using btree(oid oid_ops));
#define ForeignDataWrapperOidIndexId	3306

DECLARE_UNIQUE_INDEX(pg_foreign_data_wrapper_name_index, 3307, on pg_foreign_data_wrapper using btree(fdwname name_ops));
#define ForeignDataWrapperNameIndexId	3307

DECLARE_UNIQUE_INDEX(pg_foreign_server_oid_index, 3308, on pg_foreign_server using btree(oid oid_ops));
#define ForeignServerOidIndexId 3308

DECLARE_UNIQUE_INDEX(pg_foreign_server_name_index, 3309, on pg_foreign_server using btree(srvname name_ops));
#define ForeignServerNameIndexId	3309

DECLARE_UNIQUE_INDEX(pg_user_mapping_oid_index, 3316, on pg_user_mapping using btree(oid oid_ops));
#define UserMappingOidIndexId	3316

DECLARE_UNIQUE_INDEX(pg_user_mapping_user_server_index, 3317, on pg_user_mapping using btree(umuser oid_ops, umserver oid_ops));
#define UserMappingUserServerIndexId	3317

DECLARE_UNIQUE_INDEX(pg_foreign_table_reloid_index, 3049, on pg_foreign_table using btree(reloid oid_ops));
#define ForeignTableRelOidIndexId  3049

/* TIDYCAT_BEGIN_CODEGEN 
*/
/*
   WARNING: DO NOT MODIFY THE FOLLOWING SECTION: 
   Generated by ./tidycat.pl version 31
   on Thu May 24 14:25:58 2012
 */
/* relation id: 1260 - pg_authid 20100129 */
DECLARE_UNIQUE_INDEX(pg_authid_rolname_index, 2676, on pg_authid using btree(rolname name_ops));
#define AuthIdRolnameIndexId	2676
/* relation id: 1260 - pg_authid 20100129 */
DECLARE_UNIQUE_INDEX(pg_authid_oid_index, 2677, on pg_authid using btree(oid oid_ops));
#define AuthIdOidIndexId	2677
/* relation id: 1260 - pg_authid 20100129 */
DECLARE_INDEX(pg_authid_rolresqueue_index, 6029, on pg_authid using btree(rolresqueue oid_ops));
#define AuthIdRolResQueueIndexId	6029
/* relation id: 5010 - pg_partition 20101001 */
DECLARE_UNIQUE_INDEX(pg_partition_oid_index, 5012, on pg_partition using btree(oid oid_ops));
#define PartitionOidIndexId	5012
/* relation id: 5010 - pg_partition 20101001 */
DECLARE_INDEX(pg_partition_parrelid_index, 5013, on pg_partition using btree(parrelid oid_ops));
#define PartitionParrelidIndexId	5013
/* relation id: 5010 - pg_partition 20101001 */
DECLARE_INDEX(pg_partition_parrelid_parlevel_istemplate_index, 5017, on pg_partition using btree(parrelid oid_ops, parlevel int2_ops, paristemplate bool_ops));
#define PartitionParrelidParlevelParistemplateIndexId	5017
/* relation id: 5011 - pg_partition_rule 20101001 */
DECLARE_UNIQUE_INDEX(pg_partition_rule_oid_index, 5014, on pg_partition_rule using btree(oid oid_ops));
#define PartitionRuleOidIndexId	5014
/* relation id: 5011 - pg_partition_rule 20101001 */
DECLARE_INDEX(pg_partition_rule_parchildrelid_index, 5016, on pg_partition_rule using btree(parchildrelid oid_ops));
#define PartitionRuleParchildrelidIndexId	5016
/* relation id: 5011 - pg_partition_rule 20101001 */
DECLARE_INDEX(pg_partition_rule_parchildrelid_parparentrule_parruleord_index, 5015, on pg_partition_rule using btree(parchildrelid oid_ops, parparentrule oid_ops, parruleord int2_ops));
#define PartitionRuleParchildrelidParparentruleParruleordIndexId	5015
/* relation id: 5011 - pg_partition_rule 20101001 */
DECLARE_INDEX(pg_partition_rule_paroid_parentrule_ruleord_index, 5026, on pg_partition_rule using btree(paroid oid_ops, parparentrule oid_ops, parruleord int2_ops));
#define PartitionRuleParoidParparentruleParruleordIndexId	5026
/* relation id: 6040 - pg_exttable 20101014 */
DECLARE_UNIQUE_INDEX(pg_exttable_reloid_index, 6041, on pg_exttable using btree(reloid oid_ops));
#define ExtTableReloidIndexId	6041
/* relation id: 2600 - pg_aggregate 20101018 */
DECLARE_UNIQUE_INDEX(pg_aggregate_fnoid_index, 2650, on pg_aggregate using btree(aggfnoid oid_ops));
#define AggregateAggfnoidIndexId	2650
/* relation id: 5004 - pg_window 20101018 */
DECLARE_UNIQUE_INDEX(pg_window_fnoid_index, 5005, on pg_window using btree(winfnoid oid_ops));
#define WindowWinfnoidIndexId	5005
/* relation id: 5035 - gp_san_configuration 20101104 */
DECLARE_UNIQUE_INDEX(gp_san_config_mountid_index, 6111, on gp_san_configuration using btree(mountid int2_ops));
#define GpSanConfigMountidIndexId	6111
/* relation id: 5000 - gp_configuration 20101105 */
DECLARE_UNIQUE_INDEX(gp_configuration_content_definedprimary_index, 6101, on gp_configuration using btree(content int2_ops, definedprimary bool_ops));
#define GpConfigurationContentDefinedprimaryIndexId	6101
/* relation id: 5000 - gp_configuration 20101105 */
DECLARE_UNIQUE_INDEX(gp_configuration_dbid_index, 6102, on gp_configuration using btree(dbid int2_ops));
#define GpConfigurationDbidIndexId	6102
/* relation id: 5029 - gp_db_interfaces 20101105 */
DECLARE_INDEX(gp_db_interfaces_dbid_index, 6108, on gp_db_interfaces using btree(dbid int2_ops));
#define GpDbInterfacesDbidIndexId	6108
/* relation id: 5030 - gp_interfaces 20101105 */
DECLARE_UNIQUE_INDEX(gp_interfaces_interface_index, 6109, on gp_interfaces using btree(interfaceid int2_ops));
#define GpInterfacesInterfaceidIndexId	6109
/* relation id: 2613 - pg_largeobject 20101105 */
DECLARE_UNIQUE_INDEX(pg_largeobject_loid_pn_index, 2683, on pg_largeobject using btree(loid oid_ops, pageno int4_ops));
#define LargeObjectLoidPagenoIndexId	2683
/* relation id: 2615 - pg_namespace 20150218 */
DECLARE_UNIQUE_INDEX(pg_namespace_nspname_index, 2684, on pg_namespace using btree(nspname name_ops, nspdboid oid_ops));
#define NamespaceNspnameNspdboidIndexId	2684
/* relation id: 2615 - pg_namespace 20101105 */
DECLARE_UNIQUE_INDEX(pg_namespace_oid_index, 2685, on pg_namespace using btree(oid oid_ops));
#define NamespaceOidIndexId	2685
/* relation id: 5033 - pg_filespace_entry 20101122 */
DECLARE_INDEX(pg_filespace_entry_fs_index, 2893, on pg_filespace_entry using btree(fsefsoid oid_ops));
#define FileSpaceEntryFsefsoidIndexId	2893
/* relation id: 5033 - pg_filespace_entry 20101122 */
DECLARE_UNIQUE_INDEX(pg_filespace_entry_fsdb_index, 2894, on pg_filespace_entry using btree(fsefsoid oid_ops, fsedbid int2_ops));
#define FileSpaceEntryFsefsoidFsedbidIndexId	2894
/* relation id: 7175 - pg_extprotocol 20110526 */
DECLARE_UNIQUE_INDEX(pg_extprotocol_oid_index, 7156, on pg_extprotocol using btree(oid oid_ops));
#define ExtprotocolOidIndexId	7156
/* relation id: 7175 - pg_extprotocol 20110526 */
DECLARE_UNIQUE_INDEX(pg_extprotocol_ptcname_index, 7177, on pg_extprotocol using btree(ptcname name_ops));
#define ExtprotocolPtcnameIndexId	7177
/* relation id: 2612 - pg_language 20110607 */
DECLARE_UNIQUE_INDEX(pg_language_lanname_index, 2681, on pg_language using btree(lanname name_ops));
#define LanguageNameIndexId	2681
/* relation id: 2612 - pg_language 20110607 */
DECLARE_UNIQUE_INDEX(pg_language_oid_index, 2682, on pg_language using btree(oid oid_ops));
#define LanguageOidIndexId	2682
/* relation id: 3231 - pg_attribute_encoding 20110727 */
DECLARE_INDEX(pg_attribute_encoding_attrelid_index, 3236, on pg_attribute_encoding using btree(attrelid oid_ops));
#define AttributeEncodingAttrelidIndexId	3236
/* relation id: 3231 - pg_attribute_encoding 20110727 */
DECLARE_UNIQUE_INDEX(pg_attribute_encoding_attrelid_attnum_index, 3237, on pg_attribute_encoding using btree(attrelid oid_ops, attnum int2_ops));
#define AttributeEncodingAttrelidAttnumIndexId	3237
/* relation id: 3220 - pg_type_encoding 20110727 */
DECLARE_UNIQUE_INDEX(pg_type_encoding_typid_index, 3207, on pg_type_encoding using btree(typid oid_ops));
#define TypeEncodingTypidIndexId	3207
/* relation id: 6429 - gp_verification_history 20110609 */
DECLARE_UNIQUE_INDEX(gp_verification_history_vertoken_index, 6431, on gp_verification_history using btree(vertoken name_ops));
#define GpVerificationHistoryVertokenIndexId	6431
/* relation id: 9903 - pg_partition_encoding 20110814 */
DECLARE_UNIQUE_INDEX(pg_partition_encoding_parencoid_parencattnum_index, 9908, on pg_partition_encoding using btree(parencoid oid_ops, parencattnum int2_ops));
#define PartitionEncodingParencoidAttnumIndexId	9908
/* relation id: 3124 - pg_proc_callback 20110829 */
DECLARE_UNIQUE_INDEX(pg_proc_callback_profnoid_promethod_index, 3126, on pg_proc_callback using btree(profnoid oid_ops, promethod char_ops));
#define ProcCallbackProfnoidPromethodIndexId	3126
/* relation id: 9903 - pg_partition_encoding 20110814 */
DECLARE_INDEX(pg_partition_encoding_parencoid_index, 9909, on pg_partition_encoding using btree(parencoid oid_ops));
#define PartitionEncodingParencoidIndexId	9909
/* relation id: 3056 - pg_compression 20110901 */
DECLARE_UNIQUE_INDEX(pg_compression_oid_index, 3058, on pg_compression using btree(oid oid_ops));
#define CompressionOidIndexId	3058
/* relation id: 3056 - pg_compression 20110901 */
DECLARE_UNIQUE_INDEX(pg_compression_compname_index, 3059, on pg_compression using btree(compname name_ops));
#define CompressionCompnameIndexId	3059
/* relation id: 6112 - pg_filesystem 20130123 */
DECLARE_UNIQUE_INDEX(pg_filesystem_oid_index, 7183, on pg_filesystem using btree(oid oid_ops));
#define FileSystemOidIndexId	7183
/* relation id: 6112 - pg_filesystem 20130123 */
DECLARE_UNIQUE_INDEX(pg_filesystem_fsysname_index, 7184, on pg_filesystem using btree(fsysname name_ops));
#define FileSystemFsysnameIndexId	7184
/* relation id: 7076 - pg_remote_credentials 20140205 */
DECLARE_UNIQUE_INDEX(pg_remote_credentials_owner_service_index, 7081, on pg_remote_credentials using btree(rcowner oid_ops, rcservice text_ops));
#define RemoteCredentialsOwnerServiceIndexId	7081
/* relation id: 5036 - gp_segment_configuration 20150207 */
DECLARE_UNIQUE_INDEX(gp_segment_config_registration_order_index, 6106, on gp_segment_configuration using btree(registration_order int4_ops));
#define GpSegmentConfigRegistration_orderIndexId	6106
/* relation id: 5036 - gp_segment_configuration 20150207 */
DECLARE_INDEX(gp_segment_config_role_index, 6107, on gp_segment_configuration using btree(role char_ops));
#define GpSegmentConfigRoleIndexId	6107
/* relation id: 6119 - pg_attribute_encoding 20141112 */
DECLARE_INDEX(pg_attribute_attrelid_index, 6119, on pg_attribute using btree(attrelid oid_ops));
#define AttributeAttrelidIndexId    6119
/* relation id: 6026 - pg_resqueue 20151014 */
DECLARE_UNIQUE_INDEX(pg_resqueue_oid_index, 6027, on pg_resqueue using btree(oid oid_ops));
#define ResQueueOidIndexId	6027

/* relation id: 6026 - pg_resqueue 20151014 */
DECLARE_UNIQUE_INDEX(pg_resqueue_rsqname_index, 6028, on pg_resqueue using btree(rsqname name_ops));
#define ResQueueRsqnameIndexId	6028

/* TIDYCAT_END_CODEGEN */

/* last step of initialization script: build the indexes declared above */
BUILD_INDICES

#endif   /* INDEXING_H */
