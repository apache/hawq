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
 * joinrels.c
 *	  Routines to determine which relations should be joined
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/path/joinrels.c,v 1.81.2.3 2007/02/16 00:14:07 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"                  /* CHECK_FOR_INTERRUPTS */
#include "optimizer/joininfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"

#include "cdb/cdbdef.h"                 /* CdbSwap */


static List *make_rels_by_clause_joins(PlannerInfo *root,
						  RelOptInfo *old_rel,
						  ListCell *other_rels);
static List *make_rels_by_clauseless_joins(PlannerInfo *root,
							  RelOptInfo *old_rel,
							  ListCell *other_rels);
static void
cdb_add_subquery_join_paths(PlannerInfo    *root,
					        RelOptInfo     *joinrel,
					        RelOptInfo     *rel1,
					        RelOptInfo     *rel2,
					        JoinType        jointype,
					        JoinType        swapjointype,
					        List           *restrictlist);
static bool has_join_restriction(PlannerInfo *root, RelOptInfo *rel);
static bool has_legal_joinclause(PlannerInfo *root, RelOptInfo *rel);


/*
 * make_rels_by_joins
 *	  Consider ways to produce join relations containing exactly 'level'
 *	  jointree items.  (This is one step of the dynamic-programming method
 *	  embodied in make_one_rel_by_joins.)  Join rel nodes for each feasible
 *	  combination of lower-level rels are created and returned in a list.
 *	  Implementation paths are created for each such joinrel, too.
 *
 * level: level of rels we want to make this time.
 * joinrels[j], 1 <= j < level, is a list of rels containing j items.
 */
List *
make_rels_by_joins(PlannerInfo *root, int level, List **joinrels)
{
	List	   *result_rels = NIL;
	List	   *new_rels;
	ListCell   *r;
	int			k;

	/*
	 * First, consider left-sided and right-sided plans, in which rels of
	 * exactly level-1 member relations are joined against initial relations.
	 * We prefer to join using join clauses, but if we find a rel of level-1
	 * members that has no join clauses, we will generate Cartesian-product
	 * joins against all initial rels not already contained in it.
	 *
	 * In the first pass (level == 2), we try to join each initial rel to each
	 * initial rel that appears later in joinrels[1].  (The mirror-image joins
	 * are handled automatically by make_join_rel.)  In later passes, we try
	 * to join rels of size level-1 from joinrels[level-1] to each initial rel
	 * in joinrels[1].
	 */
	foreach(r, joinrels[level - 1])
	{
		RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);
		ListCell   *other_rels;

		if (level == 2)
			other_rels = lnext(r);		/* only consider remaining initial
										 * rels */
		else
			other_rels = list_head(joinrels[1]);		/* consider all initial
														 * rels */

		if (old_rel->joininfo != NIL || has_join_restriction(root, old_rel))
		{
			/*
			 * Note that if all available join clauses for this rel require
			 * more than one other rel, we will fail to make any joins against
			 * it here.  In most cases that's OK; it'll be considered by
			 * "bushy plan" join code in a higher-level pass where we have
			 * those other rels collected into a join rel.
			 *
			 * See also the last-ditch case below.
			 */
			new_rels = make_rels_by_clause_joins(root,
												 old_rel,
												 other_rels);
		}
		else
		{
			/*
			 * Oops, we have a relation that is not joined to any other
			 * relation, either directly or by join-order restrictions.
			 * Cartesian product time.
			 */
			new_rels = make_rels_by_clauseless_joins(root,
													 old_rel,
													 other_rels);
		}

		/*
		 * At levels above 2 we will generate the same joined relation in
		 * multiple ways --- for example (a join b) join c is the same
		 * RelOptInfo as (b join c) join a, though the second case will add a
		 * different set of Paths to it.  To avoid making extra work for
		 * subsequent passes, do not enter the same RelOptInfo into our output
		 * list multiple times.
		 */
		result_rels = list_concat_unique_ptr(result_rels, new_rels);
	}

	/*
	 * Now, consider "bushy plans" in which relations of k initial rels are
	 * joined to relations of level-k initial rels, for 2 <= k <= level-2.
	 *
	 * We only consider bushy-plan joins for pairs of rels where there is a
	 * suitable join clause (or join order restriction), in order to avoid
	 * unreasonable growth of planning time.
	 */
	for (k = 2;; k++)
	{
		int			other_level = level - k;

		/*
		 * Since make_join_rel(x, y) handles both x,y and y,x cases, we only
		 * need to go as far as the halfway point.
		 */
		if (k > other_level)
			break;

		foreach(r, joinrels[k])
		{
			RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);
			ListCell   *other_rels;
			ListCell   *r2;

			/*
			 * We can ignore clauseless joins here, *except* when they
			 * participate in join-order restrictions --- then we might have
			 * to force a bushy join plan.
			 */
			if (old_rel->joininfo == NIL &&
				!has_join_restriction(root, old_rel))
				continue;

			if (k == other_level)
				other_rels = lnext(r);	/* only consider remaining rels */
			else
				other_rels = list_head(joinrels[other_level]);

			for_each_cell(r2, other_rels)
			{
				RelOptInfo *new_rel = (RelOptInfo *) lfirst(r2);

				if (!bms_overlap(old_rel->relids, new_rel->relids))
				{
					/*
					 * OK, we can build a rel of the right level from this
					 * pair of rels.  Do so if there is at least one usable
					 * join clause or a relevant join restriction.
					 */
					if (have_relevant_joinclause(root, old_rel, new_rel) ||
						have_join_order_restriction(root, old_rel, new_rel))
					{
						RelOptInfo *jrel;

						jrel = make_join_rel(root, old_rel, new_rel);
						/* Avoid making duplicate entries ... */
						if (jrel)
							result_rels = list_append_unique_ptr(result_rels,
																 jrel);
					}
				}
			}
		}
	}

	/*
	 * Last-ditch effort: if we failed to find any usable joins so far, force
	 * a set of cartesian-product joins to be generated.  This handles the
	 * special case where all the available rels have join clauses but we
	 * cannot use any of the joins yet.  An example is
	 *
	 * SELECT * FROM a,b,c WHERE (a.f1 + b.f2 + c.f3) = 0;
	 *
	 * The join clause will be usable at level 3, but at level 2 we have no
	 * choice but to make cartesian joins.	We consider only left-sided and
	 * right-sided cartesian joins in this case (no bushy).
	 */
	if (result_rels == NIL)
	{
		/*
		 * This loop is just like the first one, except we always call
		 * make_rels_by_clauseless_joins().
		 */
		foreach(r, joinrels[level - 1])
		{
			RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);
			ListCell   *other_rels;

			if (level == 2)
				other_rels = lnext(r);	/* only consider remaining initial
										 * rels */
			else
				other_rels = list_head(joinrels[1]);	/* consider all initial
														 * rels */

			new_rels = make_rels_by_clauseless_joins(root,
													 old_rel,
													 other_rels);

			result_rels = list_concat_unique_ptr(result_rels, new_rels);
		}

		/*----------
		 * When OJs or IN clauses are involved, there may be no legal way
		 * to make an N-way join for some values of N.	For example consider
		 *
		 * SELECT ... FROM t1 WHERE
		 *	 x IN (SELECT ... FROM t2,t3 WHERE ...) AND
		 *	 y IN (SELECT ... FROM t4,t5 WHERE ...)
		 *
		 * We will flatten this query to a 5-way join problem, but there are
		 * no 4-way joins that join_is_legal() will consider legal.  We have
		 * to accept failure at level 4 and go on to discover a workable
		 * bushy plan at level 5.
		 *
		 * However, if there are no such clauses then join_is_legal() should
		 * never fail, and so the following sanity check is useful.
		 *----------
		 */
		if (result_rels == NIL &&
			root->oj_info_list == NIL && root->in_info_list == NIL)
			elog(ERROR, "failed to build any %d-way joins", level);
	}

	return result_rels;
}

/*
 * make_rels_by_clause_joins
 *	  Build joins between the given relation 'old_rel' and other relations
 *	  that participate in join clauses that 'old_rel' also participates in
 *	  (or participate in join-order restrictions with it).
 *	  The join rel nodes are returned in a list.
 *
 * 'old_rel' is the relation entry for the relation to be joined
 * 'other_rels': the first cell in a linked list containing the other
 * rels to be considered for joining
 *
 * Currently, this is only used with initial rels in other_rels, but it
 * will work for joining to joinrels too.
 */
static List *
make_rels_by_clause_joins(PlannerInfo *root,
						  RelOptInfo *old_rel,
						  ListCell *other_rels)
{
	List	   *result = NIL;
	ListCell   *l;

	for_each_cell(l, other_rels)
	{
		RelOptInfo *other_rel = (RelOptInfo *) lfirst(l);

		if (!bms_overlap(old_rel->relids, other_rel->relids) &&
			(have_relevant_joinclause(root, old_rel, other_rel) ||
			 have_join_order_restriction(root, old_rel, other_rel)))
		{
			RelOptInfo *jrel;

			jrel = make_join_rel(root, old_rel, other_rel);
			if (jrel)
				result = lcons(jrel, result);
		}
	}

	return result;
}

/*
 * make_rels_by_clauseless_joins
 *	  Given a relation 'old_rel' and a list of other relations
 *	  'other_rels', create a join relation between 'old_rel' and each
 *	  member of 'other_rels' that isn't already included in 'old_rel'.
 *	  The join rel nodes are returned in a list.
 *
 * 'old_rel' is the relation entry for the relation to be joined
 * 'other_rels': the first cell of a linked list containing the
 * other rels to be considered for joining
 *
 * Currently, this is only used with initial rels in other_rels, but it would
 * work for joining to joinrels too.
 */
static List *
make_rels_by_clauseless_joins(PlannerInfo *root,
							  RelOptInfo *old_rel,
							  ListCell *other_rels)
{
	List	   *result = NIL;
	ListCell   *i;

	for_each_cell(i, other_rels)
	{
		RelOptInfo *other_rel = (RelOptInfo *) lfirst(i);

		if (!bms_overlap(other_rel->relids, old_rel->relids))
		{
			RelOptInfo *jrel;

			jrel = make_join_rel(root, old_rel, other_rel);

			/*
			 * As long as given other_rels are distinct, don't need to test to
			 * see if jrel is already part of output list.
			 */
			if (jrel)
				result = lcons(jrel, result);
		}
	}

	return result;
}


/*
 * join_is_legal
 *	   Determine whether a proposed join is legal given the query's
 *	   join order constraints; and if it is, determine the join type.
 *
 * Caller must supply not only the two rels, but the union of their relids.
 * (We could simplify the API by computing joinrelids locally, but this
 * would be redundant work in the normal path through make_join_rel.)
 *
 * On success, *jointype_p is set to the required join type.
 */
static bool
join_is_legal(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
			  Relids joinrelids, JoinType *jointype_p)
{
	JoinType	jointype;
	bool		is_valid_inner;
	ListCell   *l;

	/*
	 * Ensure *jointype_p is set on failure return.  This is just to
	 * suppress uninitialized-variable warnings from overly anal compilers.
	 */
	*jointype_p = JOIN_INNER;

	/*
	 * If we have any outer joins, the proposed join might be illegal; and in
	 * any case we have to determine its join type.  Scan the OJ list for
	 * conflicts.
	 */
	jointype = JOIN_INNER;		/* default if no match to an OJ */
	is_valid_inner = true;

	foreach(l, root->oj_info_list)
	{
		OuterJoinInfo *ojinfo = (OuterJoinInfo *) lfirst(l);

		/*
		 * This OJ is not relevant unless its RHS overlaps the proposed join.
		 * (Check this first as a fast path for dismissing most irrelevant OJs
		 * quickly.)
		 */
		if (!bms_overlap(ojinfo->min_righthand, joinrelids))
			continue;

		/*
		 * Also, not relevant if proposed join is fully contained within RHS
		 * (ie, we're still building up the RHS).
		 */
		if (bms_is_subset(joinrelids, ojinfo->min_righthand))
			continue;

		/*
		 * Also, not relevant if OJ is already done within either input.
		 */
		if (bms_is_subset(ojinfo->min_lefthand, rel1->relids) &&
			bms_is_subset(ojinfo->min_righthand, rel1->relids))
			continue;
		if (bms_is_subset(ojinfo->min_lefthand, rel2->relids) &&
			bms_is_subset(ojinfo->min_righthand, rel2->relids))
			continue;

		/*
		 * If one input contains min_lefthand and the other contains
		 * min_righthand, then we can perform the OJ at this join.
		 *
		 * Barf if we get matches to more than one OJ (is that possible?)
		 */
		if (bms_is_subset(ojinfo->min_lefthand, rel1->relids) &&
			bms_is_subset(ojinfo->min_righthand, rel2->relids))
		{
			if (jointype != JOIN_INNER)
				return false;			/* invalid join path */
			jointype = ojinfo->join_type;
			if (jointype != JOIN_FULL && jointype != JOIN_LASJ && jointype != JOIN_LASJ_NOTIN)
				jointype = JOIN_LEFT;
		}
		else if (bms_is_subset(ojinfo->min_lefthand, rel2->relids) &&
				 bms_is_subset(ojinfo->min_righthand, rel1->relids))
		{
			if (jointype != JOIN_INNER)
				return false;			/* invalid join path */
			jointype = ojinfo->join_type;
			if (jointype != JOIN_FULL && jointype != JOIN_LASJ && jointype != JOIN_LASJ_NOTIN)
				jointype = JOIN_RIGHT;
		}
		else
		{
			/*----------
			 * Otherwise, the proposed join overlaps the RHS but isn't
			 * a valid implementation of this OJ.  It might still be
			 * a legal join, however.  If both inputs overlap the RHS,
			 * assume that it's OK.  Since the inputs presumably got past
			 * this function's checks previously, they can't overlap the
			 * LHS and their violations of the RHS boundary must represent
			 * OJs that have been determined to commute with this one.
			 * We have to allow this to work correctly in cases like
			 *		(a LEFT JOIN (b JOIN (c LEFT JOIN d)))
			 * when the c/d join has been determined to commute with the join
			 * to a, and hence d is not part of min_righthand for the upper
			 * join.  It should be legal to join b to c/d but this will appear
			 * as a violation of the upper join's RHS.
			 * Furthermore, if one input overlaps the RHS and the other does
			 * not, we should still allow the join if it is a valid
			 * implementation of some other OJ.  We have to allow this to
			 * support the associative identity
			 *		(a LJ b on Pab) LJ c ON Pbc = a LJ (b LJ c ON Pbc) on Pab
			 * since joining B directly to C violates the lower OJ's RHS.
			 * We assume that make_outerjoininfo() set things up correctly
			 * so that we'll only match to some OJ if the join is valid.
			 * Set flag here to check at bottom of loop.
			 *----------
			 */
			if (bms_overlap(rel1->relids, ojinfo->min_righthand) &&
				bms_overlap(rel2->relids, ojinfo->min_righthand))
			{
				/* seems OK */
				Assert(!bms_overlap(joinrelids, ojinfo->min_lefthand));
			}
			else
				is_valid_inner = false;
		}
	}

	/* Fail if violated some OJ's RHS and didn't match to another OJ */
	if (jointype == JOIN_INNER && !is_valid_inner)
		return false;			/* invalid join path */

	/* Join is valid */
	*jointype_p = jointype;
	return true;
}


/*
 * make_join_rel
 *	   Find or create a join RelOptInfo that represents the join of
 *	   the two given rels, and add to it path information for paths
 *	   created with the two rels as outer and inner rel.
 *	   (The join rel may already contain paths generated from other
 *	   pairs of rels that add up to the same set of base rels.)
 *
 * NB: will return NULL if attempted join is not valid.  This can happen
 * when working with outer joins, or with IN clauses that have been turned
 * into joins.
 */
RelOptInfo *
make_join_rel(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2)
{
	Relids		joinrelids;
	JoinType	jointype;
    JoinType    swapjointype;
	RelOptInfo *joinrel;
	List	   *restrictlist;

	/* This is a convenient place to check for query cancel. */
	CHECK_FOR_INTERRUPTS();

    Insist(rel1 &&
           rel2 &&
           rel1->cheapest_total_path &&
           rel2->cheapest_total_path);

	/* We should never try to join two overlapping sets of rels. */
	Assert(!bms_overlap(rel1->relids, rel2->relids));

	/* Construct Relids set that identifies the joinrel. */
	joinrelids = bms_union(rel1->relids, rel2->relids);

	/* Check validity and determine join type. */
	if (!join_is_legal(root, rel1, rel2, joinrelids, &jointype))
	{
		/* invalid join path */
		bms_free(joinrelids);
		return NULL;
	}

	/*
	 * Find or build the join RelOptInfo, and compute the restrictlist that
	 * goes with this particular joining.
	 */
	joinrel = build_join_rel(root, joinrelids, rel1, rel2, jointype,
							 &restrictlist);

    /* Reversed jointype is useful when rel2 becomes outer and rel1 is inner. */
    swapjointype = (jointype == JOIN_LEFT)  ? JOIN_RIGHT
                 : (jointype == JOIN_RIGHT) ? JOIN_LEFT
                 :                            jointype;

    /*
     * CDB: Consider plans in which an upstream subquery's duplicate
     * suppression is either postponed yet further downstream, or subsumed
     * in this join by the use of the JOIN_IN technique on behalf of another
     * subquery.
     */
    if ((rel1->dedup_info && rel1->dedup_info->later_dedup_pathlist) ||
        (rel2->dedup_info && rel2->dedup_info->later_dedup_pathlist))
        cdb_add_subquery_join_paths(root,
                                    joinrel,
                                    rel1,
                                    rel2,
                                    jointype,
                                    swapjointype,
                                    restrictlist);

    /*
	 * For antijoins, the outer and inner rel are fixed.
	 */
	else if (jointype == JOIN_LASJ || jointype == JOIN_LASJ_NOTIN)
	{
        add_paths_to_joinrel(root, joinrel, rel1, rel2, jointype, restrictlist);
	}
    /*
	 * Consider paths using each rel as both outer and inner.
	 */
    else
    {
        add_paths_to_joinrel(root, joinrel, rel1, rel2, jointype, restrictlist);
        add_paths_to_joinrel(root, joinrel, rel2, rel1, swapjointype, restrictlist);
    }

	bms_free(joinrelids);
	return joinrel;
}


/*
 * cdb_add_subquery_join_paths
 *
 * Each input rel may have a special pathlist containing paths in which some
 * flattened subqueries have been completely evaluated except for duplicate
 * suppression.  Here we consider joins taking input from those paths.  By
 * postponing duplicate suppression until additional subqueries are brought
 * into the join, a single duplicate suppression can cover multiple subqueries.
 */
void
cdb_add_subquery_join_paths(PlannerInfo    *root,
					        RelOptInfo     *joinrel,
					        RelOptInfo     *rel1,
					        RelOptInfo     *rel2,
					        JoinType        jointype,
					        JoinType        swapjointype,
					        List           *restrictlist)
{
    CdbRelDedupInfo    *dedup1 = rel1->dedup_info;
    CdbRelDedupInfo    *dedup2 = rel2->dedup_info;
    List               *save1_pathlist;
    Path               *save1_cheapest_startup;
    Path               *save1_cheapest_total;
    List               *save2_pathlist;
    Path               *save2_cheapest_startup;
    Path               *save2_cheapest_total;

    /* If only one input's later_dedup_pathlist is nonempty, let it be rel2. */
    if (!dedup2 ||
        !dedup2->later_dedup_pathlist)
    {
        CdbSwap(RelOptInfo*, rel1, rel2);
        CdbSwap(CdbRelDedupInfo*, dedup1, dedup2);
        CdbSwap(JoinType, jointype, swapjointype);
        Assert(dedup2 && dedup2->later_dedup_pathlist);
    }

    /* Consider joins between rel1's main paths and rel2's main paths. */
    if (rel1->pathlist && rel2->pathlist)
    {
        add_paths_to_joinrel(root, joinrel, rel1, rel2, jointype, restrictlist);
        add_paths_to_joinrel(root, joinrel, rel2, rel1, swapjointype, restrictlist);
    }

    /* Save rel2's main pathlist ptrs. */
    save2_pathlist = rel2->pathlist;
    save2_cheapest_startup = rel2->cheapest_startup_path;
    save2_cheapest_total = rel2->cheapest_total_path;

    /* Swap in rel2's later_dedup_pathlist. */
    rel2->pathlist = dedup2->later_dedup_pathlist;
    rel2->cheapest_startup_path = dedup2->cheapest_startup_path;
    rel2->cheapest_total_path = dedup2->cheapest_total_path;

    /* Consider joins between rel1's main paths and rel2's later_dedup paths. */
    if (rel1->pathlist)
    {
        add_paths_to_joinrel(root, joinrel, rel1, rel2, jointype, restrictlist);
        add_paths_to_joinrel(root, joinrel, rel2, rel1, swapjointype, restrictlist);
    }

    /* Finished if rel1 doesn't have later_dedup paths. */
    if (!dedup1 ||
        !dedup1->later_dedup_pathlist)
    {
        /* Restore rel2's main pathlist ptrs. */
        rel2->pathlist = save2_pathlist;
        rel2->cheapest_startup_path = save2_cheapest_startup;
        rel2->cheapest_total_path = save2_cheapest_total;
        return;
    }

    /* Save rel1's main pathlist ptrs. */
    save1_pathlist = rel1->pathlist;
    save1_cheapest_startup = rel1->cheapest_startup_path;
    save1_cheapest_total = rel1->cheapest_total_path;

    /* Swap in rel1's later_dedup_pathlist. */
    rel1->pathlist = dedup1->later_dedup_pathlist;
    rel1->cheapest_startup_path = dedup1->cheapest_startup_path;
    rel1->cheapest_total_path = dedup1->cheapest_total_path;

    /* Consider joins between later_dedup paths of both rel1 and rel2. */
    add_paths_to_joinrel(root, joinrel, rel1, rel2, jointype, restrictlist);
    add_paths_to_joinrel(root, joinrel, rel2, rel1, swapjointype, restrictlist);

    /* Restore rel2's main pathlist ptrs. */
    rel2->pathlist = save2_pathlist;
    rel2->cheapest_startup_path = save2_cheapest_startup;
    rel2->cheapest_total_path = save2_cheapest_total;

    /* Consider joins between rel1's later_dedup paths and rel2's main paths. */
    if (rel2->pathlist)
    {
        add_paths_to_joinrel(root, joinrel, rel1, rel2, jointype, restrictlist);
        add_paths_to_joinrel(root, joinrel, rel2, rel1, swapjointype, restrictlist);
    }

    /* Restore rel1's main pathlist ptrs. */
    rel1->pathlist = save1_pathlist;
    rel1->cheapest_startup_path = save1_cheapest_startup;
    rel1->cheapest_total_path = save1_cheapest_total;
}                               /* cdb_add_subquery_join_paths */


/*
 * have_join_order_restriction
 *		Detect whether the two relations should be joined to satisfy
 *		a join-order restriction arising from outer joins or IN clauses.
 *
 * In practice this is always used with have_relevant_joinclause(), and so
 * could be merged with that function, but it seems clearer to separate the
 * two concerns.  We need these tests because there are degenerate cases where
 * a clauseless join must be performed to satisfy join-order restrictions.
 *
 * Note: this is only a problem if one side of a degenerate outer join
 * contains multiple rels, or a clauseless join is required within an IN's
 * RHS; else we will find a join path via the "last ditch" case in
 * make_rels_by_joins().  We could dispense with this test if we were willing
 * to try bushy plans in the "last ditch" case, but that seems much less
 * efficient.
 */
bool
have_join_order_restriction(PlannerInfo *root,
							RelOptInfo *rel1, RelOptInfo *rel2)
{
	bool		result = false;
	ListCell   *l;

	/*
	 * It's possible that the rels correspond to the left and right sides
	 * of a degenerate outer join, that is, one with no joinclause mentioning
	 * the non-nullable side; in which case we should force the join to occur.
	 *
	 * Also, the two rels could represent a clauseless join that has to be
	 * completed to build up the LHS or RHS of an outer join.
	 */
	foreach(l, root->oj_info_list)
	{
		OuterJoinInfo *ojinfo = (OuterJoinInfo *) lfirst(l);

		/* ignore full joins --- other mechanisms handle them */
		if (ojinfo->join_type == JOIN_FULL)
			continue;

		/* Can we perform the OJ with these rels? */
		if (bms_is_subset(ojinfo->min_lefthand, rel1->relids) &&
			bms_is_subset(ojinfo->min_righthand, rel2->relids))
		{
			result = true;
			break;
		}
		if (bms_is_subset(ojinfo->min_lefthand, rel2->relids) &&
			bms_is_subset(ojinfo->min_righthand, rel1->relids))
		{
			result = true;
			break;
		}

		/*
		 * Might we need to join these rels to complete the RHS?  We have
		 * to use "overlap" tests since either rel might include a lower OJ
		 * that has been proven to commute with this one.
		 */
		if (bms_overlap(ojinfo->min_righthand, rel1->relids) &&
			bms_overlap(ojinfo->min_righthand, rel2->relids))
		{
			result = true;
			break;
		}

		/* Likewise for the LHS. */
		if (bms_overlap(ojinfo->min_lefthand, rel1->relids) &&
			bms_overlap(ojinfo->min_lefthand, rel2->relids))
		{
			result = true;
			break;
		}
	}

	/*
	 * We do not force the join to occur if either input rel can legally
	 * be joined to anything else using joinclauses.  This essentially
	 * means that clauseless bushy joins are put off as long as possible.
	 * The reason is that when there is a join order restriction high up
	 * in the join tree (that is, with many rels inside the LHS or RHS),
	 * we would otherwise expend lots of effort considering very stupid
	 * join combinations within its LHS or RHS.
	 */
	if (result)
	{
		if (has_legal_joinclause(root, rel1) ||
			has_legal_joinclause(root, rel2))
			result = false;
	}

    /*
     * In CDB, unlike PostgreSQL, flattened subqueries do not restrict the
     * join sequence; we aren't in danger of being unable to join all the
     * tables.  Still, an early cross-product (clauseless join) might
     * sometimes enable the consideration of pre-join duplicate elimination
     * (JOIN_IN or JOIN_UNIQUE).  Someday we should consider a well-chosen
     * set of early cross products.  For now, limit the search space by
     * means of a simple heuristic.
     */
	foreach(l, root->in_info_list)
	{
		InClauseInfo *ininfo = (InClauseInfo *) lfirst(l);

        /* Does the subquery RHS consist of exactly rel1 + rel2? */
        if (bms_is_subset(rel1->relids, ininfo->righthand) &&
            bms_is_subset(rel2->relids, ininfo->righthand) &&
            bms_num_members(rel1->relids) + bms_num_members(rel2->relids) ==
                bms_num_members(ininfo->righthand))
		{
			result = true;
			break;
		}
	}

	return result;
}


/*
 * has_join_restriction
 *		Detect whether the specified relation has join-order restrictions
 *		due to being inside an outer join or an IN (sub-SELECT).
 *
 * Essentially, this tests whether have_join_order_restriction() could
 * succeed with this rel and some other one.  It's OK if we sometimes
 * say "true" incorrectly.  (Therefore, we don't bother with the relatively
 * expensive has_legal_joinclause test.)
 */
static bool
has_join_restriction(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell   *l;

	foreach(l, root->oj_info_list)
	{
		OuterJoinInfo *ojinfo = (OuterJoinInfo *) lfirst(l);

		/* ignore full joins --- other mechanisms preserve their ordering */
		if (ojinfo->join_type == JOIN_FULL)
			continue;

		/* ignore if OJ is already contained in rel */
		if (bms_is_subset(ojinfo->min_lefthand, rel->relids) &&
			bms_is_subset(ojinfo->min_righthand, rel->relids))
			continue;

		/* restricted if it overlaps LHS or RHS, but doesn't contain OJ */
		if (bms_overlap(ojinfo->min_lefthand, rel->relids) ||
			bms_overlap(ojinfo->min_righthand, rel->relids))
			return true;
	}

	foreach(l, root->in_info_list)
	{
		InClauseInfo *ininfo = (InClauseInfo *) lfirst(l);

        /* CDB: Consider cross product if subquery RHS = rel + some other rel */
        if (bms_is_subset(rel->relids, ininfo->righthand) &&
            !bms_equal(rel->relids, ininfo->righthand))
			return true;
	}

	return false;
}


/*
 * has_legal_joinclause
 *		Detect whether the specified relation can legally be joined
 *		to any other rels using join clauses.
 *
 * We consider only joins to single other relations in the current
 * initial_rels list.  This is sufficient to get a "true" result in most real
 * queries, and an occasional erroneous "false" will only cost a bit more
 * planning time.  The reason for this limitation is that considering joins to
 * other joins would require proving that the other join rel can legally be
 * formed, which seems like too much trouble for something that's only a
 * heuristic to save planning time.  (Note: we must look at initial_rels
 * and not all of the query, since when we are planning a sub-joinlist we
 * may be forced to make clauseless joins within initial_rels even though
 * there are join clauses linking to other parts of the query.)
 */
static bool
has_legal_joinclause(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell   *lc;

	foreach(lc, root->initial_rels)
	{
		RelOptInfo *rel2 = (RelOptInfo *) lfirst(lc);

		/* ignore rels that are already in "rel" */
		if (bms_overlap(rel->relids, rel2->relids))
			continue;

		if (have_relevant_joinclause(root, rel, rel2))
		{
			Relids		joinrelids;
			JoinType	jointype;

			/* join_is_legal needs relids of the union */
			joinrelids = bms_union(rel->relids, rel2->relids);

			if (join_is_legal(root, rel, rel2, joinrelids, &jointype))
			{
				/* Yes, this will work */
				bms_free(joinrelids);
				return true;
			}

			bms_free(joinrelids);
		}
	}

	return false;
}
