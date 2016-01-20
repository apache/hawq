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
 * prepjointree.c
 *	  Planner preprocessing for subqueries and join tree manipulation.
 *
 * NOTE: the intended sequence for invoking these operations is
 *		pull_up_IN_clauses
 *		pull_up_subqueries
 *		do expression preprocessing (including flattening JOIN alias vars)
 *		reduce_outer_joins
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/prep/prepjointree.c,v 1.44 2006/10/04 00:29:54 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_expr.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"

#include "cdb/cdbsubselect.h"           /* cdbsubselect_flatten_sublinks() */


typedef struct reduce_outer_joins_state
{
	Relids		relids;			/* base relids within this subtree */
	bool		contains_outer; /* does subtree contain outer join(s)? */
	List	   *sub_states;		/* List of states for subtree components */
} reduce_outer_joins_state;

static void pull_up_fromlist_subqueries(PlannerInfo    *root,
                                        List          **inout_fromlist,
				                        bool            below_outer_join);
static Node *pull_up_simple_subquery(PlannerInfo *root, Node *jtnode,
						RangeTblEntry *rte,
						bool below_outer_join,
						bool append_rel_member);
static bool is_simple_subquery(PlannerInfo *root, Query *subquery);
static bool has_nullable_targetlist(Query *subquery);
static bool is_safe_append_member(Query *subquery);
static void resolvenew_in_jointree(Node *jtnode, int varno,
					   RangeTblEntry *rte, List *subtlist);
static reduce_outer_joins_state *reduce_outer_joins_pass1(Node *jtnode);
static void reduce_outer_joins_pass2(Node *jtnode,
						 reduce_outer_joins_state *state,
						 PlannerInfo *root,
						 Relids nonnullable_rels);
static void fix_in_clause_relids(List *in_info_list, int varno,
					 Relids subrelids);
static void fix_append_rel_relids(List *append_rel_list, Index varno,
					  Relids subrelids);
static Node *find_jointree_node_for_rel(Node *jtnode, int relid);

extern void UpdateScatterClause(Query *query, List *newtlist);

/*
 * pull_up_IN_clauses
 *
 * CDB: This function has been moved into cdbsubselect.c.
 */

/*
 * pull_up_subqueries
 *		Look for subqueries in the rangetable that can be pulled up into
 *		the parent query.  If the subquery has no special features like
 *		grouping/aggregation then we can merge it into the parent's jointree.
 *		Also, subqueries that are simple UNION ALL structures can be
 *		converted into "append relations".
 *
 * below_outer_join is true if this jointree node is within the nullable
 * side of an outer join.  This restricts what we can do.
 *
 * append_rel_member is true if we are looking at a member subquery of
 * an append relation.	This puts some different restrictions on what
 * we can do.
 *
 * A tricky aspect of this code is that if we pull up a subquery we have
 * to replace Vars that reference the subquery's outputs throughout the
 * parent query, including quals attached to jointree nodes above the one
 * we are currently processing!  We handle this by being careful not to
 * change the jointree structure while recursing: no nodes other than
 * subquery RangeTblRef entries will be replaced.  Also, we can't turn
 * ResolveNew loose on the whole jointree, because it'll return a mutated
 * copy of the tree; we have to invoke it just on the quals, instead.
 */
Node *
pull_up_subqueries(PlannerInfo *root, Node *jtnode,
				   bool below_outer_join, bool append_rel_member)
{
    if (jtnode == NULL)
		return NULL;
	if (IsA(jtnode, RangeTblRef))
	{
		int			varno = ((RangeTblRef *) jtnode)->rtindex;
		RangeTblEntry *rte = rt_fetch(varno, root->parse->rtable);

		/*
		 * Is this a subquery RTE, and if so, is the subquery simple enough to
		 * pull up?  (If not, do nothing at this node.)
		 *
		 * If we are inside an outer join, only pull up subqueries whose
		 * targetlists are nullable --- otherwise substituting their tlist
		 * entries for upper Var references would do the wrong thing (the
		 * results wouldn't become NULL when they're supposed to).
		 *
		 * XXX This could be improved by generating pseudo-variables for such
		 * expressions; we'd have to figure out how to get the pseudo-
		 * variables evaluated at the right place in the modified plan tree.
		 * Fix it someday.
		 *
		 * If we are looking at an append-relation member, we can't pull it up
		 * unless is_safe_append_member says so.
		 */
		if (rte->rtekind == RTE_SUBQUERY && !rte->forceDistRandom &&
			is_simple_subquery(root, rte->subquery) &&
			(!below_outer_join || has_nullable_targetlist(rte->subquery)) &&
			(!append_rel_member || is_safe_append_member(rte->subquery)))
			return pull_up_simple_subquery(root, jtnode, rte,
										   below_outer_join,
										   append_rel_member);

		/* PG:
		 * Alternatively, is it a simple UNION ALL subquery?  If so, flatten
		 * into an "append relation".  We can do this regardless of
		 * nullability considerations since this transformation does not
		 * result in propagating non-Var expressions into upper levels of the
		 * query.
		 *
		 * It's also safe to do this regardless of whether this query is
		 * itself an appendrel member.	(If you're thinking we should try to
		 * flatten the two levels of appendrel together, you're right; but we
		 * handle that in set_append_rel_pathlist, not here.)
		 * 
		 * GPDB: 
		 * Flattening to an append relation works in PG but is not safe to do in GPDB. 
		 * A "simple" UNION ALL may involve relations with different loci and would require resolving
		 * locus issues. It is preferable to avoid pulling up simple UNION ALL in GPDB.
		 */
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;

		Assert(!append_rel_member);
        pull_up_fromlist_subqueries(root, &f->fromlist, below_outer_join);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		Assert(!append_rel_member);
		/* Recurse, being careful to tell myself when inside outer join */
		switch (j->jointype)
		{
			case JOIN_INNER:
				j->larg = pull_up_subqueries(root, j->larg,
											 below_outer_join, false);
				j->rarg = pull_up_subqueries(root, j->rarg,
											 below_outer_join, false);
				break;
			case JOIN_LEFT:
			case JOIN_LASJ:
			case JOIN_LASJ_NOTIN:
				j->larg = pull_up_subqueries(root, j->larg,
											 below_outer_join, false);
				j->rarg = pull_up_subqueries(root, j->rarg,
											 true, false);
				break;
			case JOIN_FULL:
				j->larg = pull_up_subqueries(root, j->larg,
											 true, false);
				j->rarg = pull_up_subqueries(root, j->rarg,
											 true, false);
				break;
			case JOIN_RIGHT:
				j->larg = pull_up_subqueries(root, j->larg,
											 true, false);
				j->rarg = pull_up_subqueries(root, j->rarg,
											 below_outer_join, false);
				break;
			default:
				elog(ERROR, "unrecognized join type: %d",
					 (int) j->jointype);
				break;
		}

        /*
         * CDB: If subqueries from the JOIN...ON search condition were
         * flattened, 'subqfromlist' is a list of RangeTblRef nodes to be
         * included in the cross product with larg and rarg.  Try to pull up
         * the referenced subqueries.  For outer joins, let below_outer_join
         * be true, because the subquery tables belong in the null-augmented
         * side of the JOIN (right side of LEFT JOIN).
         */
        if (j->subqfromlist)
            pull_up_fromlist_subqueries(root, &j->subqfromlist,
                                        below_outer_join || (j->jointype != JOIN_INNER));
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
	return jtnode;
}


/*
 * pull_up_fromlist_subqueries
 *		Attempt to pull up subqueries in a List of jointree nodes.
 */
static void
pull_up_fromlist_subqueries(PlannerInfo    *root,
                            List          **inout_fromlist,
				            bool            below_outer_join)
{
    ListCell   *l;

    foreach(l, *inout_fromlist)
    {
        Node   *oldkid = (Node *)lfirst(l);
        Node   *newkid = pull_up_subqueries(root, oldkid,
											below_outer_join, false);

        lfirst(l) = newkid;
    }
}                               /* pull_up_fromlist_subqueries */


/*
 * pull_up_simple_subquery
 *		Attempt to pull up a single simple subquery.
 *
 * jtnode is a RangeTblRef that has been tentatively identified as a simple
 * subquery by pull_up_subqueries.	We return the replacement jointree node,
 * or jtnode itself if we determine that the subquery can't be pulled up after
 * all.
 */
static Node *
pull_up_simple_subquery(PlannerInfo *root, Node *jtnode, RangeTblEntry *rte,
						bool below_outer_join, bool append_rel_member)
{
	Query	   *parse = root->parse;
	int			varno = ((RangeTblRef *) jtnode)->rtindex;
	Query	   *subquery;
	PlannerInfo *subroot;
	int			rtoffset;
	List	   *subtlist;
	ListCell   *rt;
    ListCell   *cell;

	/*
	 * Need a modifiable copy of the subquery to hack on.  Even if we didn't
	 * sometimes choose not to pull up below, we must do this to avoid
	 * problems if the same subquery is referenced from multiple jointree
	 * items (which can't happen normally, but might after rule rewriting).
	 */
	subquery = copyObject(rte->subquery);

	/*
	 * Create a PlannerInfo data structure for this subquery.
	 *
	 * NOTE: the next few steps should match the first processing in
	 * subquery_planner().	Can we refactor to avoid code duplication, or
	 * would that just make things uglier?
	 */
	subroot = makeNode(PlannerInfo);
	subroot->parse = subquery;
	subroot->in_info_list = NIL;
	subroot->append_rel_list = NIL;

	subroot->list_cteplaninfo = NIL;
	if (subroot->parse->cteList != NIL)
	{
		subroot->list_cteplaninfo = init_list_cteplaninfo(list_length(subroot->parse->cteList));
	}

    /* CDB: Stash subquery jointree relids before flattening subqueries. */
    subroot->currlevel_relids = get_relids_in_jointree((Node *)subquery->jointree);
    
    /* Ensure that jointree has been normalized. See normalize_query_jointree_mutator() */
    AssertImply(subquery->jointree->fromlist, list_length(subquery->jointree->fromlist) == 1);
    
    subroot->config = CopyPlannerConfig(root->config);
	/* CDB: Clear fallback */
	subroot->config->mpp_trying_fallback_plan = false;

	/*
	 * Pull up any IN clauses within the subquery's WHERE, so that we don't
	 * leave unoptimized INs behind.
	 */
	if (subquery->hasSubLinks)
        cdbsubselect_flatten_sublinks(subroot, (Node *)subquery);

	/*
	 * Recursively pull up the subquery's subqueries, so that
	 * pull_up_subqueries' processing is complete for its jointree and
	 * rangetable.
	 *
	 * Note: below_outer_join = false is correct here even if we are within an
	 * outer join in the upper query; the lower query starts with a clean
	 * slate for outer-join semantics.	Likewise, we say we aren't handling an
	 * appendrel member.
	 */
	subquery->jointree = (FromExpr *)
		pull_up_subqueries(subroot, (Node *) subquery->jointree, false, false);

	/*
	 * Now we must recheck whether the subquery is still simple enough to pull
	 * up.	If not, abandon processing it.
	 *
	 * We don't really need to recheck all the conditions involved, but it's
	 * easier just to keep this "if" looking the same as the one in
	 * pull_up_subqueries.
	 */
	if (is_simple_subquery(root, subquery) &&
		(!below_outer_join || has_nullable_targetlist(subquery)) &&
		(!append_rel_member || is_safe_append_member(subquery)))
	{
		/* good to go */
	}
	else
	{
		/*
		 * Give up, return unmodified RangeTblRef.
		 *
		 * Note: The work we just did will be redone when the subquery gets
		 * planned on its own.	Perhaps we could avoid that by storing the
		 * modified subquery back into the rangetable, but I'm not gonna risk
		 * it now.
		 */
		return jtnode;
	}

    /* CDB: If parent RTE belongs to subquery's query level, children do too. */
    foreach (cell, subroot->append_rel_list)
    {
        AppendRelInfo  *appinfo = (AppendRelInfo *)lfirst(cell);

        if (bms_is_member(appinfo->parent_relid, subroot->currlevel_relids))
            subroot->currlevel_relids = bms_add_member(subroot->currlevel_relids,
                                                       appinfo->child_relid);
    }

	/*
	 * Adjust level-0 varnos in subquery so that we can append its rangetable
	 * to upper query's.  We have to fix the subquery's in_info_list and
	 * append_rel_list, as well.
	 */
	rtoffset = list_length(parse->rtable);
	OffsetVarNodes((Node *) subquery, rtoffset, 0);
	OffsetVarNodes((Node *) subroot->in_info_list, rtoffset, 0);
	OffsetVarNodes((Node *) subroot->append_rel_list, rtoffset, 0);

	/*
	 * Upper-level vars in subquery are now one level closer to their parent
	 * than before.
	 */
	IncrementVarSublevelsUp((Node *) subquery, -1, 1);
	IncrementVarSublevelsUp((Node *) subroot->in_info_list, -1, 1);
	IncrementVarSublevelsUp((Node *) subroot->append_rel_list, -1, 1);

	/*
	 * Replace all of the top query's references to the subquery's outputs
	 * with copies of the adjusted subtlist items, being careful not to
	 * replace any of the jointree structure. (This'd be a lot cleaner if we
	 * could use query_tree_mutator.)
	 */
	subtlist = subquery->targetList;

	List *newTList = (List *)
		ResolveNew((Node *) parse->targetList,
				   varno, 0, rte,
				   subtlist, CMD_SELECT, 0);

	if (parse->scatterClause)
	{
		UpdateScatterClause(parse, newTList);
	}

	parse->targetList = newTList;

	parse->returningList = (List *)
		ResolveNew((Node *) parse->returningList,
				   varno, 0, rte,
				   subtlist, CMD_SELECT, 0);
	resolvenew_in_jointree((Node *) parse->jointree, varno,
						   rte, subtlist);
	Assert(parse->setOperations == NULL);
	parse->havingQual =
		ResolveNew(parse->havingQual,
				   varno, 0, rte,
				   subtlist, CMD_SELECT, 0);
	root->in_info_list = (List *)
		ResolveNew((Node *) root->in_info_list,
				   varno, 0, rte,
				   subtlist, CMD_SELECT, 0);
	root->append_rel_list = (List *)
		ResolveNew((Node *) root->append_rel_list,
				   varno, 0, rte,
				   subtlist, CMD_SELECT, 0);

	if (parse->windowClause)
	{
		foreach(cell, parse->windowClause)
		{
			WindowSpec *win_spec = (WindowSpec *)lfirst(cell);
			if (win_spec->frame)
			{
				WindowFrame *frame = win_spec->frame;
				
				if (frame->trail)
					frame->trail->val = 
						ResolveNew((Node *)frame->trail->val,
								   varno, 0, rte,
								   subtlist, CMD_SELECT, 0);
				if (frame->lead)
					frame->lead->val =
						ResolveNew((Node *)frame->lead->val,
								   varno, 0, rte,
								   subtlist, CMD_SELECT, 0);
			}
		}
	}

	foreach(rt, parse->rtable)
	{
		RangeTblEntry *otherrte = (RangeTblEntry *) lfirst(rt);

		if (otherrte->rtekind == RTE_JOIN)
			otherrte->joinaliasvars = (List *)
				ResolveNew((Node *) otherrte->joinaliasvars,
						   varno, 0, rte,
						   subtlist, CMD_SELECT, 0);

		else if (otherrte->rtekind == RTE_SUBQUERY && rte != otherrte)
		{
			otherrte->subquery = (Query *)
				ResolveNew((Node *) otherrte->subquery,
							varno, 1, rte, /* here the sublevels_up can only be 1, because if larger than 1,
											  then the sublink is multilevel correlated, and cannot be pulled
											  up to be a subquery range table; while on the other hand, we
											  cannot directly put a subquery which refer to other relations
											  of the same level after FROM. */
							subtlist, CMD_SELECT, 0);
		}
	}

	/*
	 * Now append the adjusted rtable entries to upper query. (We hold off
	 * until after fixing the upper rtable entries; no point in running that
	 * code on the subquery ones too.)
	 */
	parse->rtable = list_concat(parse->rtable, subquery->rtable);

	/*
	 * Pull up any FOR UPDATE/SHARE markers, too.  (OffsetVarNodes already
	 * adjusted the marker rtindexes, so just concat the lists.)
	 */
	parse->rowMarks = list_concat(parse->rowMarks, subquery->rowMarks);

    /*
     * CDB: Fix current query level's FROM clause relid set if the subquery
     * was in the FROM clause of current query (not a flattened sublink).
     */
    if (bms_is_member(varno, root->currlevel_relids))
    {
        int     subrelid;

        root->currlevel_relids = bms_del_member(root->currlevel_relids, varno);
        bms_foreach(subrelid, subroot->currlevel_relids)
            root->currlevel_relids = bms_add_member(root->currlevel_relids,
                                                    subrelid + rtoffset);
    }

	/*
	 * We also have to fix the relid sets of any parent InClauseInfo nodes.
	 * (This could perhaps be done by ResolveNew, but it would clutter that
	 * routine's API unreasonably.)
	 *
	 * Likewise, relids appearing in AppendRelInfo nodes have to be fixed (but
	 * we took care of their translated_vars lists above).	We already checked
	 * that this won't require introducing multiple subrelids into the
	 * single-slot AppendRelInfo structs.
	 */
	if (root->in_info_list || root->append_rel_list)
	{
		Relids		subrelids;

		subrelids = get_relids_in_jointree((Node *) subquery->jointree);
		fix_in_clause_relids(root->in_info_list, varno, subrelids);
		fix_append_rel_relids(root->append_rel_list, varno, subrelids);
	}

	/*
	 * And now add any subquery InClauseInfos and AppendRelInfos to our lists.
	 */
	root->in_info_list = list_concat(root->in_info_list,
									 subroot->in_info_list);
	root->append_rel_list = list_concat(root->append_rel_list,
										subroot->append_rel_list);

	/*
	 * We don't have to do the equivalent bookkeeping for outer-join info,
	 * because that hasn't been set up yet.
	 */
	Assert(root->oj_info_list == NIL);
	Assert(subroot->oj_info_list == NIL);

	/*
	 * Miscellaneous housekeeping.
	 */
	parse->hasSubLinks |= subquery->hasSubLinks;
	/* subquery won't be pulled up if it hasAggs, so no work there */


    /*
     * CDB: Wipe old RTE so subquery parse tree won't be sent to QEs.
     */
    Assert(rte->rtekind == RTE_SUBQUERY);
    rte->rtekind = RTE_VOID;
    rte->subquery = NULL;
    rte->alias = NULL;
    rte->eref = NULL;

	/*
	 * Return the adjusted subquery jointree to replace the RangeTblRef entry
	 * in parent's jointree.
	 */
	return (Node *) subquery->jointree;
}


/*
 * is_simple_subquery
 *	  Check a subquery in the range table to see if it's simple enough
 *	  to pull up into the parent query.
 */
static bool
is_simple_subquery(PlannerInfo *root, Query *subquery)
{
	/*
	 * Let's just make sure it's a valid subselect ...
	 */
	if (!IsA(subquery, Query) ||
		subquery->commandType != CMD_SELECT ||
		subquery->intoClause != NULL)
		elog(ERROR, "subquery is bogus");

	/*
	 * Can't currently pull up a query with setops (unless it's simple UNION
	 * ALL, which is handled by a different code path). Maybe after querytree
	 * redesign...
	 */
	if (subquery->setOperations)
		return false;

	/*
	 * Can't pull up a subquery involving grouping, aggregation, windowing,
	 * sorting, limiting, or WITH.
	 */
	if (subquery->hasAggs ||
	    subquery->hasWindFuncs ||
		subquery->groupClause ||
		subquery->havingQual ||
		subquery->windowClause ||
		subquery->sortClause ||
		subquery->distinctClause ||
		subquery->limitOffset ||
		subquery->limitCount ||
		subquery->cteList ||
		root->parse->cteList)
		return false;

	/*
	 * Don't pull up a subquery that has any set-returning functions in its
	 * targetlist.	Otherwise we might well wind up inserting set-returning
	 * functions into places where they mustn't go, such as quals of higher
	 * queries.
	 */
	if (expression_returns_set((Node *) subquery->targetList))
		return false;

	/*
	 * Don't pull up a subquery that has any volatile functions in its
	 * targetlist.	Otherwise we might introduce multiple evaluations of these
	 * functions, if they get copied to multiple places in the upper query,
	 * leading to surprising results.
	 */
	if (contain_volatile_functions((Node *) subquery->targetList))
		return false;

	/*
	 * Hack: don't try to pull up a subquery with an empty jointree.
	 * query_planner() will correctly generate a Result plan for a jointree
	 * that's totally empty, but I don't think the right things happen if an
	 * empty FromExpr appears lower down in a jointree. Not worth working hard
	 * on this, just to collapse SubqueryScan/Result into Result...
	 */
	if (subquery->jointree->fromlist == NIL)
		return false;

	return true;
}



/*
 * has_nullable_targetlist
 *	  Check a subquery in the range table to see if all the non-junk
 *	  targetlist items are simple variables or strict functions of simple
 *	  variables (and, hence, will correctly go to NULL when examined above
 *	  the point of an outer join).
 *
 * NOTE: it would be correct (and useful) to ignore output columns that aren't
 * actually referenced by the enclosing query ... but we do not have that
 * information available at this point.
 */
static bool
has_nullable_targetlist(Query *subquery)
{
	ListCell   *l;

	foreach(l, subquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		/* ignore resjunk columns */
		if (tle->resjunk)
			continue;

		/* Must contain a Var of current level */
		if (!contain_vars_of_level((Node *) tle->expr, 0))
			return false;

		/* Must not contain any non-strict constructs */
		if (contain_nonstrict_functions((Node *) tle->expr))
			return false;

		/* This one's OK, keep scanning */
	}
	return true;
}

/*
 * is_safe_append_member
 *	  Check a subquery that is a leaf of a UNION ALL appendrel to see if it's
 *	  safe to pull up.
 */
static bool
is_safe_append_member(Query *subquery)
{
	FromExpr   *jtnode;
	ListCell   *l;

	/*
	 * It's only safe to pull up the child if its jointree contains exactly
	 * one RTE, else the AppendRelInfo data structure breaks. The one base RTE
	 * could be buried in several levels of FromExpr, however.
	 *
	 * Also, the child can't have any WHERE quals because there's no place to
	 * put them in an appendrel.  (This is a bit annoying...) If we didn't
	 * need to check this, we'd just test whether get_relids_in_jointree()
	 * yields a singleton set, to be more consistent with the coding of
	 * fix_append_rel_relids().
	 */
	jtnode = subquery->jointree;
	while (IsA(jtnode, FromExpr))
	{
		if (jtnode->quals != NULL)
			return false;
		if (list_length(jtnode->fromlist) != 1)
			return false;
		jtnode = linitial(jtnode->fromlist);
	}
	if (!IsA(jtnode, RangeTblRef))
		return false;

	/*
	 * XXX For the moment we also have to insist that the subquery's tlist
	 * includes only simple Vars.  This is pretty annoying, but fixing it
	 * seems to require nontrivial changes --- mainly because joinrel tlists
	 * are presently assumed to contain only Vars.	Perhaps a pseudo-variable
	 * mechanism similar to the one speculated about in pull_up_subqueries'
	 * comments would help?  FIXME someday.
	 */
	foreach(l, subquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->resjunk)
			continue;
		if (!(tle->expr && IsA(tle->expr, Var)))
			return false;
	}

	return true;
}

/*
 * Helper routine for pull_up_subqueries: do ResolveNew on every expression
 * in the jointree, without changing the jointree structure itself.  Ugly,
 * but there's no other way...
 */
static void
resolvenew_in_jointree(Node *jtnode, int varno,
					   RangeTblEntry *rte, List *subtlist)
{
	ListCell   *l;

	if (jtnode == NULL)
		return;
	if (IsA(jtnode, RangeTblRef))
	{
		/* nothing to do here */
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;

		foreach(l, f->fromlist)
			resolvenew_in_jointree(lfirst(l), varno, rte, subtlist);
		f->quals = ResolveNew(f->quals,
							  varno, 0, rte,
							  subtlist, CMD_SELECT, 0);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		resolvenew_in_jointree(j->larg, varno, rte, subtlist);
		resolvenew_in_jointree(j->rarg, varno, rte, subtlist);
		foreach(l, j->subqfromlist)
			resolvenew_in_jointree(lfirst(l), varno, rte, subtlist);
		j->quals = ResolveNew(j->quals,
							  varno, 0, rte,
							  subtlist, CMD_SELECT, 0);

		/*
		 * We don't bother to update the colvars list, since it won't be used
		 * again ...
		 */
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
}

/*
 * reduce_outer_joins
 *		Attempt to reduce outer joins to plain inner joins.
 *
 * The idea here is that given a query like
 *		SELECT ... FROM a LEFT JOIN b ON (...) WHERE b.y = 42;
 * we can reduce the LEFT JOIN to a plain JOIN if the "=" operator in WHERE
 * is strict.  The strict operator will always return NULL, causing the outer
 * WHERE to fail, on any row where the LEFT JOIN filled in NULLs for b's
 * columns.  Therefore, there's no need for the join to produce null-extended
 * rows in the first place --- which makes it a plain join not an outer join.
 * (This scenario may not be very likely in a query written out by hand, but
 * it's reasonably likely when pushing quals down into complex views.)
 *
 * More generally, an outer join can be reduced in strength if there is a
 * strict qual above it in the qual tree that constrains a Var from the
 * nullable side of the join to be non-null.  (For FULL joins this applies
 * to each side separately.)
 *
 * To ease recognition of strict qual clauses, we require this routine to be
 * run after expression preprocessing (i.e., qual canonicalization and JOIN
 * alias-var expansion).
 */
void
reduce_outer_joins(PlannerInfo *root)
{
	reduce_outer_joins_state *state;

	/*
	 * To avoid doing strictness checks on more quals than necessary, we want
	 * to stop descending the jointree as soon as there are no outer joins
	 * below our current point.  This consideration forces a two-pass process.
	 * The first pass gathers information about which base rels appear below
	 * each side of each join clause, and about whether there are outer
	 * join(s) below each side of each join clause. The second pass examines
	 * qual clauses and changes join types as it descends the tree.
	 */
	state = reduce_outer_joins_pass1((Node *) root->parse->jointree);

	/* planner.c shouldn't have called me if no outer joins */
	if (state == NULL || !state->contains_outer)
		elog(ERROR, "so where are the outer joins?");

	reduce_outer_joins_pass2((Node *) root->parse->jointree,
							 state, root, NULL);
}

/*
 * reduce_outer_joins_pass1 - phase 1 data collection
 *
 * Returns a state node describing the given jointree node.
 */
static reduce_outer_joins_state *
reduce_outer_joins_pass1(Node *jtnode)
{
	reduce_outer_joins_state *result;
	ListCell   *l;

	result = (reduce_outer_joins_state *)
		palloc(sizeof(reduce_outer_joins_state));
	result->relids = NULL;
	result->contains_outer = false;
	result->sub_states = NIL;

	if (jtnode == NULL)
		return result;
	if (IsA(jtnode, RangeTblRef))
	{
		int			varno = ((RangeTblRef *) jtnode)->rtindex;

		result->relids = bms_make_singleton(varno);
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;

		foreach(l, f->fromlist)
		{
			reduce_outer_joins_state *sub_state;

			sub_state = reduce_outer_joins_pass1(lfirst(l));
			result->relids = bms_add_members(result->relids,
											 sub_state->relids);
			result->contains_outer |= sub_state->contains_outer;
			result->sub_states = lappend(result->sub_states, sub_state);
		}
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;
		reduce_outer_joins_state *sub_state;

		/* join's own RT index is not wanted in result->relids */
		if (IS_OUTER_JOIN(j->jointype))
			result->contains_outer = true;

		sub_state = reduce_outer_joins_pass1(j->larg);
		result->relids = bms_add_members(result->relids,
										 sub_state->relids);
		result->contains_outer |= sub_state->contains_outer;
		result->sub_states = lappend(result->sub_states, sub_state);

		sub_state = reduce_outer_joins_pass1(j->rarg);
		result->relids = bms_add_members(result->relids,
										 sub_state->relids);
		result->contains_outer |= sub_state->contains_outer;
		result->sub_states = lappend(result->sub_states, sub_state);

		foreach(l, j->subqfromlist)
		{
			sub_state = reduce_outer_joins_pass1(lfirst(l));
			result->relids = bms_add_members(result->relids,
											 sub_state->relids);
			result->contains_outer |= sub_state->contains_outer;
			result->sub_states = lappend(result->sub_states, sub_state);
		}
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
	return result;
}

/*
 * reduce_outer_joins_pass2 - phase 2 processing
 *
 *	jtnode: current jointree node
 *	state: state data collected by phase 1 for this node
 *	root: toplevel planner state
 *	nonnullable_rels: set of base relids forced non-null by upper quals
 */
static void
reduce_outer_joins_pass2(Node *jtnode,
						 reduce_outer_joins_state *state,
						 PlannerInfo *root,
						 Relids nonnullable_rels)
{
	ListCell   *l;
	ListCell   *s;

	/*
	 * pass 2 should never descend as far as an empty subnode or base rel,
	 * because it's only called on subtrees marked as contains_outer.
	 */
	if (jtnode == NULL)
		elog(ERROR, "reached empty jointree");
	if (IsA(jtnode, RangeTblRef))
		elog(ERROR, "reached base rel");
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		Relids		pass_nonnullable;

		/* Scan quals to see if we can add any nonnullability constraints */
		pass_nonnullable = find_nonnullable_rels(f->quals);
		pass_nonnullable = bms_add_members(pass_nonnullable,
										   nonnullable_rels);
		/* And recurse --- but only into interesting subtrees */
		Assert(list_length(f->fromlist) == list_length(state->sub_states));
		forboth(l, f->fromlist, s, state->sub_states)
		{
			reduce_outer_joins_state *sub_state = lfirst(s);

			if (sub_state->contains_outer)
				reduce_outer_joins_pass2(lfirst(l), sub_state, root,
										 pass_nonnullable);
		}
		bms_free(pass_nonnullable);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;
		int			rtindex = j->rtindex;
		JoinType	jointype = j->jointype;
		reduce_outer_joins_state *left_state = linitial(state->sub_states);
		reduce_outer_joins_state *right_state = lsecond(state->sub_states);
		reduce_outer_joins_state *sub_state;

		/* Can we simplify this join? */
		switch (jointype)
		{
			case JOIN_LEFT:
				if (bms_overlap(nonnullable_rels, right_state->relids))
					jointype = JOIN_INNER;
				break;
			case JOIN_RIGHT:
				if (bms_overlap(nonnullable_rels, left_state->relids))
					jointype = JOIN_INNER;
				break;
			case JOIN_FULL:
				if (bms_overlap(nonnullable_rels, left_state->relids))
				{
					if (bms_overlap(nonnullable_rels, right_state->relids))
						jointype = JOIN_INNER;
					else
						jointype = JOIN_LEFT;
				}
				else
				{
					if (bms_overlap(nonnullable_rels, right_state->relids))
						jointype = JOIN_RIGHT;
				}
				break;
			default:
				break;
		}
		if (jointype != j->jointype)
		{
			/* apply the change to both jointree node and RTE */
			RangeTblEntry *rte = rt_fetch(rtindex, root->parse->rtable);

			Assert(rte->rtekind == RTE_JOIN);
			Assert(rte->jointype == j->jointype);
			rte->jointype = j->jointype = jointype;
		}

		/* Only recurse if there's more to do below here */
        foreach(l, state->sub_states)
        {
            sub_state = (reduce_outer_joins_state *)lfirst(l);
            if (sub_state->contains_outer)
                break;
        }
		if (l)
		{
			Relids		local_nonnullable;
			Relids		pass_nonnullable;

			/*
			 * If this join is (now) inner, we can add any nonnullability
			 * constraints its quals provide to those we got from above. But
			 * if it is outer, we can only pass down the local constraints
			 * into the nullable side, because an outer join never eliminates
			 * any rows from its non-nullable side.  If it's a FULL join then
			 * it doesn't eliminate anything from either side.
			 */
			if (jointype != JOIN_FULL)
			{
				local_nonnullable = find_nonnullable_rels(j->quals);
				local_nonnullable = bms_add_members(local_nonnullable,
													nonnullable_rels);
			}
			else
				local_nonnullable = NULL;		/* no use in calculating it */

			if (left_state->contains_outer)
			{
				if (jointype == JOIN_INNER || jointype == JOIN_RIGHT)
					pass_nonnullable = local_nonnullable;
				else
					pass_nonnullable = nonnullable_rels;
				reduce_outer_joins_pass2(j->larg, left_state, root,
										 pass_nonnullable);
			}
			if (right_state->contains_outer)
			{
				if (jointype == JOIN_INNER || jointype == JOIN_LEFT)
					pass_nonnullable = local_nonnullable;
				else
					pass_nonnullable = nonnullable_rels;
				reduce_outer_joins_pass2(j->rarg, right_state, root,
										 pass_nonnullable);
			}

            /*
             * CDB: Simplify outer joins pulled up from flattened subqueries.
             * For a left or right outer join, the subqfromlist items belong
             * to the null-augmented side; so we pass local_nonnullable down
             * regardless of the jointype.  (For FULL JOIN, subqfromlist is
             * always empty.)
             */
            s = lnext(lnext(list_head(state->sub_states)));
            foreach(l, j->subqfromlist)
            {
                sub_state = (reduce_outer_joins_state *)lfirst(s);
                if (sub_state->contains_outer)
				    reduce_outer_joins_pass2(lfirst(l), sub_state, root,
										     local_nonnullable);
                s = lnext(s);
            }

			bms_free(local_nonnullable);
		}
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
}

/*
 * fix_in_clause_relids: update RT-index sets of InClauseInfo nodes
 *
 * When we pull up a subquery, any InClauseInfo references to the subquery's
 * RT index have to be replaced by the set of substituted relids.
 *
 * We assume we may modify the InClauseInfo nodes in-place.
 */
static void
fix_in_clause_relids(List *in_info_list, int varno, Relids subrelids)
{
	ListCell   *l;

	foreach(l, in_info_list)
	{
		InClauseInfo *ininfo = (InClauseInfo *) lfirst(l);

		if (bms_is_member(varno, ininfo->righthand))
		{
			ininfo->righthand = bms_del_member(ininfo->righthand, varno);
			ininfo->righthand = bms_add_members(ininfo->righthand, subrelids);
		}
	}
}

/*
 * fix_append_rel_relids: update RT-index fields of AppendRelInfo nodes
 *
 * When we pull up a subquery, any AppendRelInfo references to the subquery's
 * RT index have to be replaced by the substituted relid (and there had better
 * be only one).
 *
 * We assume we may modify the AppendRelInfo nodes in-place.
 */
static void
fix_append_rel_relids(List *append_rel_list, Index varno, Relids subrelids)
{
	ListCell   *l;
	int			subvarno = -1;

	/*
	 * We only want to extract the member relid once, but we mustn't fail
	 * immediately if there are multiple members; it could be that none of the
	 * AppendRelInfo nodes refer to it.  So compute it on first use. Note that
	 * bms_singleton_member will complain if set is not singleton.
	 */
	foreach(l, append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);

		/* The parent_relid shouldn't ever be a pullup target */
		Assert(appinfo->parent_relid != varno);

		if (appinfo->child_relid == varno)
		{
			if (subvarno < 0)
				subvarno = bms_singleton_member(subrelids);
			appinfo->child_relid = subvarno;
		}
	}
}

/*
 * get_relids_in_jointree: get set of base RT indexes present in a jointree
 */
Relids
get_relids_in_jointree(Node *jtnode)
{
	Relids		result = NULL;
	ListCell   *l;

	if (jtnode == NULL)
		return result;
	if (IsA(jtnode, RangeTblRef))
	{
		int			varno = ((RangeTblRef *) jtnode)->rtindex;

		result = bms_make_singleton(varno);
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;

		foreach(l, f->fromlist)
		{
			result = bms_join(result,
							  get_relids_in_jointree(lfirst(l)));
		}
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		/* join's own RT index is not wanted in result */
		result = get_relids_in_jointree(j->larg);
		result = bms_join(result, get_relids_in_jointree(j->rarg));

		foreach(l, j->subqfromlist)
			result = bms_join(result, get_relids_in_jointree((Node *)lfirst(l)));
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
	return result;
}

/*
 * get_relids_for_join: get set of base RT indexes making up a join
 */
Relids
get_relids_for_join(PlannerInfo *root, int joinrelid)
{
	Node	   *jtnode;

	jtnode = find_jointree_node_for_rel((Node *) root->parse->jointree,
										joinrelid);
	if (!jtnode)
		elog(ERROR, "could not find join node %d", joinrelid);
	return get_relids_in_jointree(jtnode);
}

/*
 * find_jointree_node_for_rel: locate jointree node for a base or join RT index
 *
 * Returns NULL if not found
 */
static Node *
find_jointree_node_for_rel(Node *jtnode, int relid)
{
	ListCell   *l;

	if (jtnode == NULL)
		return NULL;
	if (IsA(jtnode, RangeTblRef))
	{
		int			varno = ((RangeTblRef *) jtnode)->rtindex;

		if (relid == varno)
			return jtnode;
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;

		foreach(l, f->fromlist)
		{
			jtnode = find_jointree_node_for_rel(lfirst(l), relid);
			if (jtnode)
				return jtnode;
		}
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		if (relid == j->rtindex)
			return jtnode;
		jtnode = find_jointree_node_for_rel(j->larg, relid);
		if (jtnode)
			return jtnode;
		jtnode = find_jointree_node_for_rel(j->rarg, relid);
		if (jtnode)
			return jtnode;

		foreach(l, j->subqfromlist)
		{
			jtnode = find_jointree_node_for_rel(lfirst(l), relid);
			if (jtnode)
				return jtnode;
		}
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
	return NULL;
}

/*
 * init_list_cteplaninfo
 *   Create a list of CtePlanInfos of size 'numCtes', and initialize each CtePlanInfo.
 */
List *
init_list_cteplaninfo(int numCtes)
{
	List *list_cteplaninfo = NULL;
	
	for (int cteNo = 0; cteNo < numCtes; cteNo++)
	{
		CtePlanInfo *ctePlanInfo = palloc0(sizeof(CtePlanInfo));
		list_cteplaninfo = lappend(list_cteplaninfo, ctePlanInfo);
	}

	return list_cteplaninfo;
	
}
