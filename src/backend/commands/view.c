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
 * view.c
 *	  use rewrite rules to construct views
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/view.c,v 1.98 2006/10/04 00:29:52 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteSupport.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"


#include "cdb/cdbdisp.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbcat.h"


static void checkViewTupleDesc(TupleDesc newdesc, TupleDesc olddesc);
static bool isViewOnTempTable_walker(Node *node, void *context);

/*---------------------------------------------------------------------
 * isViewOnTempTable
 *
 * Returns true iff any of the relations underlying this view are
 * temporary tables.
 *---------------------------------------------------------------------
 */
static bool
isViewOnTempTable(Query *viewParse)
{
	return isViewOnTempTable_walker((Node *) viewParse, NULL);
}

static bool
isViewOnTempTable_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		ListCell   *rtable;

		foreach(rtable, query->rtable)
		{
			RangeTblEntry *rte = lfirst(rtable);

			if (rte->rtekind == RTE_RELATION)
			{
				Relation	rel = heap_open(rte->relid, AccessShareLock);
				bool		istemp = rel->rd_istemp;

				heap_close(rel, AccessShareLock);
				if (istemp)
					return true;
			}
		}

		return query_tree_walker(query,
								 isViewOnTempTable_walker,
								 context,
								 QTW_IGNORE_JOINALIASES);
	}

	return expression_tree_walker(node,
								  isViewOnTempTable_walker,
								  context);
}

/*---------------------------------------------------------------------
 * DefineVirtualRelation
 *
 * Create the "view" relation. `DefineRelation' does all the work,
 * we just provide the correct arguments ... at least when we're
 * creating a view.  If we're updating an existing view, we have to
 * work harder.
 *---------------------------------------------------------------------
 */
static Oid
DefineVirtualRelation(const RangeVar *relation, List *tlist, bool replace, Oid viewOid, Oid * comptypeOid)
{
	Oid			namespaceId;
	CreateStmt *createStmt = makeNode(CreateStmt);
	List	   *attrList;
	ListCell   *t;

	createStmt->oidInfo.relOid = viewOid;
	if (comptypeOid)
		createStmt->oidInfo.comptypeOid = *comptypeOid;
	else
		createStmt->oidInfo.comptypeOid = 0;
	createStmt->oidInfo.toastOid = 0;
	createStmt->oidInfo.toastIndexOid = 0;
	createStmt->oidInfo.aosegOid = 0;
	createStmt->oidInfo.aosegIndexOid = 0;
	createStmt->oidInfo.aoblkdirOid = 0;
	createStmt->oidInfo.aoblkdirIndexOid = 0;
	createStmt->ownerid = GetUserId();

	/*
	 * create a list of ColumnDef nodes based on the names and types of the
	 * (non-junk) targetlist items from the view's SELECT list.
	 */
	attrList = NIL;
	foreach(t, tlist)
	{
		TargetEntry *tle = lfirst(t);

		if (!tle->resjunk)
		{
			ColumnDef  *def = makeNode(ColumnDef);

			def->colname = pstrdup(tle->resname);
			def->typname = makeTypeNameFromOid(exprType((Node *) tle->expr),
											 exprTypmod((Node *) tle->expr));
			def->inhcount = 0;
			def->is_local = true;
			def->is_not_null = false;
			def->raw_default = NULL;
			def->cooked_default = NULL;
			def->constraints = NIL;

			attrList = lappend(attrList, def);
		}
	}

	if (attrList == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("view must have at least one column")));

	/*
	 * Check to see if we want to replace an existing view.
	 */
	namespaceId = RangeVarGetCreationNamespace(relation);
	viewOid = get_relname_relid(relation->relname, namespaceId);

	if (OidIsValid(viewOid) && replace)
	{
		Relation	rel;
		TupleDesc	descriptor;

		/*
		 * Yes.  Get exclusive lock on the existing view ...
		 */
		rel = relation_open(viewOid, AccessExclusiveLock);

		/*
		 * Make sure it *is* a view, and do permissions checks.
		 */
		if (rel->rd_rel->relkind != RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a view",
							RelationGetRelationName(rel)),
									   errOmitLocation(true)));

		if (!pg_class_ownercheck(viewOid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
						   RelationGetRelationName(rel));

		/*
		 * Due to the namespace visibility rules for temporary objects, we
		 * should only end up replacing a temporary view with another
		 * temporary view, and vice versa.
		 */
		Assert(relation->istemp == rel->rd_istemp);

		/*
		 * Create a tuple descriptor to compare against the existing view, and
		 * verify it matches.
		 */
		descriptor = BuildDescForRelation(attrList);
		checkViewTupleDesc(descriptor, rel->rd_att);

		/* During upgrade mode, use the "alter table add column" code to add new
		 * columns to the view definition.
		 */
		if (gp_upgrade_mode && descriptor->natts > rel->rd_att->natts)
		{
			int i = 0;
			foreach_with_count(t, attrList, i)
				if (i >= rel->rd_att->natts)
					ATAddColumn(rel, (ColumnDef*)lfirst(t));
		}

		/*
		 * Seems okay, so return the OID of the pre-existing view.
		 */
		relation_close(rel, NoLock);	/* keep the lock! */

		return viewOid;
	}
	else
	{
		Oid newviewOid;
		/*
		 * now set the parameters for keys/inheritance etc. All of these are
		 * uninteresting for views...
		 */
		createStmt->relation = (RangeVar *) relation;
		createStmt->tableElts = attrList;
		createStmt->inhRelations = NIL;
		createStmt->constraints = NIL;
		createStmt->options = list_make1(defWithOids(false));
		createStmt->oncommit = ONCOMMIT_NOOP;
		createStmt->tablespacename = NULL;
		createStmt->relKind = RELKIND_VIEW;

		/*
		 * finally create the relation (this will error out if there's an
		 * existing view, so we don't need more code to complain if "replace"
		 * is false).
		 */
		newviewOid =  DefineRelation(createStmt, RELKIND_VIEW, RELSTORAGE_VIRTUAL);
		if(comptypeOid)
			*comptypeOid = createStmt->oidInfo.comptypeOid;
		return newviewOid;
	}
}

/*
 * Verify that tupledesc associated with proposed new view definition
 * matches tupledesc of old view.  This is basically a cut-down version
 * of equalTupleDescs(), with code added to generate specific complaints.
 */
static void
checkViewTupleDesc(TupleDesc newdesc, TupleDesc olddesc)
{
	int			i;

	/* The number of columns in the view can't change,
	 * except during upgrade where the number of col
	 * can increase.
	 */
	if (!gp_upgrade_mode && newdesc->natts != olddesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot change number of columns in view"),
						   errOmitLocation(true)));
	if (gp_upgrade_mode && newdesc->natts < olddesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot reduce the number of columns in view"),
						   errOmitLocation(true)));

	/* we can ignore tdhasoid */

	for (i = 0; i < olddesc->natts; i++)
	{
		Form_pg_attribute newattr = newdesc->attrs[i];
		Form_pg_attribute oldattr = olddesc->attrs[i];

		/* XXX not right, but we don't support DROP COL on view anyway */
		if (newattr->attisdropped != oldattr->attisdropped)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot change number of columns in view"),
							   errOmitLocation(true)));

		if (strcmp(NameStr(newattr->attname), NameStr(oldattr->attname)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot change name of view column \"%s\"",
							NameStr(oldattr->attname)),
									   errOmitLocation(true)));
		/* XXX would it be safe to allow atttypmod to change?  Not sure */
		if (newattr->atttypid != oldattr->atttypid ||
			newattr->atttypmod != oldattr->atttypmod)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot change data type of view column \"%s\"",
							NameStr(oldattr->attname)),
									   errOmitLocation(true)));
		/* We can ignore the remaining attributes of an attribute... */
	}

	/*
	 * We ignore the constraint fields.  The new view desc can't have any
	 * constraints, and the only ones that could be on the old view are
	 * defaults, which we are happy to leave in place.
	 */
}

static RuleStmt *
FormViewRetrieveRule(const RangeVar *view, Query *viewParse, bool replace, Oid rewriteOid)
{
	RuleStmt   *rule;

	/*
	 * Create a RuleStmt that corresponds to the suitable rewrite rule args
	 * for DefineQueryRewrite();
	 */
	rule = makeNode(RuleStmt);
	rule->relation = copyObject((RangeVar *) view);
	rule->rulename = pstrdup(ViewSelectRuleName);
	rule->whereClause = NULL;
	rule->event = CMD_SELECT;
	rule->instead = true;
	rule->actions = list_make1(viewParse);
	rule->replace = replace;
	rule->ruleOid = rewriteOid;

	return rule;
}

static void
DefineViewRules(const RangeVar *view, Query *viewParse, bool replace, Oid* rewriteOid)
{
	RuleStmt   *retrieve_rule;

#ifdef NOTYET
	RuleStmt   *replace_rule;
	RuleStmt   *append_rule;
	RuleStmt   *delete_rule;
#endif

	retrieve_rule = FormViewRetrieveRule(view, viewParse, replace, *rewriteOid);

#ifdef NOTYET
	replace_rule = FormViewReplaceRule(view, viewParse);
	append_rule = FormViewAppendRule(view, viewParse);
	delete_rule = FormViewDeleteRule(view, viewParse);
#endif

	DefineQueryRewrite(retrieve_rule);
	*rewriteOid = retrieve_rule->ruleOid;

#ifdef NOTYET
	DefineQueryRewrite(replace_rule);
	DefineQueryRewrite(append_rule);
	DefineQueryRewrite(delete_rule);
#endif

}

/*---------------------------------------------------------------
 * UpdateRangeTableOfViewParse
 *
 * Update the range table of the given parsetree.
 * This update consists of adding two new entries IN THE BEGINNING
 * of the range table (otherwise the rule system will die a slow,
 * horrible and painful death, and we do not want that now, do we?)
 * one for the OLD relation and one for the NEW one (both of
 * them refer in fact to the "view" relation).
 *
 * Of course we must also increase the 'varnos' of all the Var nodes
 * by 2...
 *
 * These extra RT entries are not actually used in the query,
 * except for run-time permission checking.
 *---------------------------------------------------------------
 */
static Query *
UpdateRangeTableOfViewParse(Oid viewOid, Query *viewParse)
{
	Relation	viewRel;
	List	   *new_rt;
	RangeTblEntry *rt_entry1,
			   *rt_entry2;

	/*
	 * Make a copy of the given parsetree.	It's not so much that we don't
	 * want to scribble on our input, it's that the parser has a bad habit of
	 * outputting multiple links to the same subtree for constructs like
	 * BETWEEN, and we mustn't have OffsetVarNodes increment the varno of a
	 * Var node twice.	copyObject will expand any multiply-referenced subtree
	 * into multiple copies.
	 */
	viewParse = (Query *) copyObject(viewParse);

	/* need to open the rel for addRangeTableEntryForRelation */
	viewRel = relation_open(viewOid, AccessShareLock);

	/*
	 * Create the 2 new range table entries and form the new range table...
	 * OLD first, then NEW....
	 */
	rt_entry1 = addRangeTableEntryForRelation(NULL, viewRel,
											  makeAlias("*OLD*", NIL),
											  false, false);
	rt_entry2 = addRangeTableEntryForRelation(NULL, viewRel,
											  makeAlias("*NEW*", NIL),
											  false, false);
	/* Must override addRangeTableEntry's default access-check flags */
	rt_entry1->requiredPerms = 0;
	rt_entry2->requiredPerms = 0;

	new_rt = lcons(rt_entry1, lcons(rt_entry2, viewParse->rtable));

	viewParse->rtable = new_rt;

	/*
	 * Now offset all var nodes by 2, and jointree RT indexes too.
	 */
	OffsetVarNodes((Node *) viewParse, 2, 0);

	relation_close(viewRel, AccessShareLock);

	return viewParse;
}

/*-------------------------------------------------------------------
 * DefineView
 *
 *		- takes a "viewname", "parsetree" pair and then
 *		1)		construct the "virtual" relation
 *		2)		commit the command but NOT the transaction,
 *				so that the relation exists
 *				before the rules are defined.
 *		2)		define the "n" rules specified in the PRS2 paper
 *				over the "virtual" relation
 *-------------------------------------------------------------------
 */
void
DefineView(ViewStmt *stmt)
{
	Oid			viewOid = stmt->relOid;
	RangeVar   *view = stmt->view;
	Query	   *viewParse = stmt->query;
	bool		replace = stmt->replace;

	if (Gp_role != GP_ROLE_EXECUTE)
		viewOid = 0;
	/*
	 * If the user didn't explicitly ask for a temporary view, check whether
	 * we need one implicitly.
	 */
	if (!view->istemp)
	{
		view->istemp = isViewOnTempTable(viewParse);
		if (view->istemp)
			if (Gp_role != GP_ROLE_EXECUTE)
			ereport(NOTICE,
					(errmsg("view \"%s\" will be a temporary view",
							view->relname),
									   errOmitLocation(true)));
	}

	/*
	 * Create the view relation
	 *
	 * NOTE: if it already exists and replace is false, the xact will be
	 * aborted.
	 */
	viewOid = DefineVirtualRelation(view, viewParse->targetList, replace, viewOid, &stmt->comptypeOid);

	stmt->relOid = viewOid;

	/*
	 * The relation we have just created is not visible to any other commands
	 * running with the same transaction & command id. So, increment the
	 * command id counter (but do NOT pfree any memory!!!!)
	 */
	CommandCounterIncrement();

	/*
	 * The range table of 'viewParse' does not contain entries for the "OLD"
	 * and "NEW" relations. So... add them!
	 */
	viewParse = UpdateRangeTableOfViewParse(viewOid, viewParse);

	/*
	 * Now create the rules associated with the view.
	 */
	DefineViewRules(view, viewParse, replace, &stmt->rewriteOid);
}

/*
 * RemoveView
 *
 * Remove a view given its name
 *
 * We just have to drop the relation; the associated rules will be
 * cleaned up automatically.
 */
void
RemoveView(const RangeVar *view, DropBehavior behavior)
{
	Oid			viewOid;
	ObjectAddress object;

	viewOid = RangeVarGetRelid(view, false, false /*allowHcatalog*/);

	object.classId = RelationRelationId;
	object.objectId = viewOid;
	object.objectSubId = 0;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		LockRelationOid(RelationRelationId, RowExclusiveLock);
		LockRelationOid(TypeRelationId, RowExclusiveLock);
		LockRelationOid(DependRelationId, RowExclusiveLock);
	}

	performDeletion(&object, behavior);

}
