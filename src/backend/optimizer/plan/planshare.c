/* 
 * planshare.c
 * 		Plan shared plan
 *
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
 *
 */

#include "postgres.h"

#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/subselect.h"
#include "optimizer/planshare.h"

#include "cdb/cdbgroup.h"					/* cdbpathlocus_collocates() */
#include "cdb/cdbpath.h"
#include "cdb/cdbsetop.h"					/* make_motion... routines */
#include "cdb/cdbmutate.h" 					/* add_slice_to_motion */
#include "cdb/cdbvars.h"

int get_plan_share_id(Plan *p)
{
	if(IsA(p, Material))
		return ((Material *) p)->share_id;
	
	if(IsA(p, Sort))
		return ((Sort *) p)->share_id;

	Assert(IsA(p, ShareInputScan));
	return ((ShareInputScan *) p)->share_id;
}

void set_plan_share_id(Plan *p, int share_id)
{
	if(IsA(p, Material))
		((Material *) p)->share_id = share_id;
	else if(IsA(p, Sort))
		((Sort *) p)->share_id = share_id;
	else
	{
		Assert(IsA(p, ShareInputScan));
		((ShareInputScan *) p)->share_id = share_id;
	}
}

ShareType get_plan_share_type(Plan *p)
{
	if(IsA(p, Material))
		return ((Material *) p)->share_type;
	
	if(IsA(p, Sort))
		return ((Sort *) p)->share_type ;

	Assert(IsA(p, ShareInputScan));
	return ((ShareInputScan *) p)->share_type;
}

void set_plan_share_type(Plan *p, ShareType share_type)
{
	if(IsA(p, Material))
		((Material *) p)->share_type = share_type;
	else if(IsA(p, Sort))
		((Sort *) p)->share_type = share_type;
	else
	{
		Assert(IsA(p, ShareInputScan));
		((ShareInputScan *) p)->share_type = share_type;
	}
}

void set_plan_share_type_xslice(Plan *p)
{
	ShareType st = get_plan_share_type(p);
	if(st == SHARE_MATERIAL)
		set_plan_share_type(p, SHARE_MATERIAL_XSLICE);
	else
	{
		Assert(st == SHARE_SORT);
		set_plan_share_type(p, SHARE_SORT_XSLICE);
	}
}

int get_plan_driver_slice(Plan *p)
{
	if(IsA(p, Material))
		return ((Material *) p)->driver_slice;

	Assert(IsA(p, Sort));
	return ((Sort *) p)->driver_slice;
}

void set_plan_driver_slice(Plan *p, int slice)
{
	if(IsA(p, Material))
		((Material *) p)->driver_slice = slice;
	else
	{
		Assert(IsA(p, Sort));
		((Sort *) p)->driver_slice = slice;
	}
}

static void incr_plan_nsharer(Plan *p)
{
	if(IsA(p, Material))
		((Material *) p)->nsharer++;
	else
	{
		Assert(IsA(p, Sort));
		((Sort *) p)->nsharer++;
	}
}

void incr_plan_nsharer_xslice(Plan *p)
{
	if(IsA(p, Material))
		((Material *) p)->nsharer_xslice++;
	else
	{
		Assert(IsA(p, Sort));
		((Sort *) p)->nsharer_xslice++;
	}
}

static ShareInputScan *make_shareinputscan(PlannerInfo *root, Plan *inputplan) 
{
	ShareInputScan *sisc = NULL;
	Path sipath;

	Assert(IsA(inputplan, Material) || IsA(inputplan, Sort));

	sisc = makeNode(ShareInputScan);
	incr_plan_nsharer(inputplan);


	sisc->plan.targetlist = copyObject(inputplan->targetlist);
	sisc->plan.lefttree = inputplan;
	sisc->plan.flow = copyObject(inputplan->flow); 

	set_plan_share_type((Plan *) sisc, get_plan_share_type(inputplan));
	set_plan_share_id((Plan *) sisc, get_plan_share_id(inputplan));
	sisc->driver_slice = -1;

	sisc->plan.qual = NIL;
	sisc->plan.righttree = NULL;

	cost_shareinputscan(&sipath, root, inputplan->total_cost, inputplan->plan_rows, inputplan->plan_width);

	sisc->plan.startup_cost = sipath.startup_cost;
	sisc->plan.total_cost = sipath.total_cost; 
	sisc->plan.plan_rows = inputplan->plan_rows;
	sisc->plan.plan_width = inputplan->plan_width;

	sisc->plan.extParam = bms_copy(inputplan->extParam);
	sisc->plan.allParam = bms_copy(inputplan->allParam);

	return sisc;
}

List *share_plan(PlannerInfo *root, Plan *common, int numpartners)
{
	List *shared_nodes = NULL;
	ShareType stype;
	Plan *shared = common;
	bool xslice = false;
	int i;

	Assert(numpartners > 0);
	
	if(numpartners == 1)
	{
		shared_nodes = lappend(shared_nodes, common);
		return shared_nodes;
	}

	if (IsA(common, ShareInputScan))
	{
		shared = common->lefttree;
	}
	
	else if(IsA(common, Material))
	{
		Material *m = (Material *) common;

		Assert(m->share_type == SHARE_NOTSHARED);
		Assert(m->share_id == SHARE_ID_NOT_SHARED);

		stype = xslice ? SHARE_MATERIAL_XSLICE : SHARE_MATERIAL; 
		m->share_id = SHARE_ID_NOT_ASSIGNED;
		m->share_type = stype;
	}
	else if (IsA(common, Sort))
	{
		Sort *s = (Sort *) common;

		Assert(s->share_type == SHARE_NOTSHARED);
		stype = xslice ? SHARE_SORT_XSLICE : SHARE_SORT;
		s->share_id = SHARE_ID_NOT_ASSIGNED;
		s->share_type = stype;
	}
	else
	{
		Path matpath;
		Material *m = make_material(common);
		shared = (Plan *) m;

		cost_material(&matpath, root, common->total_cost, common->plan_rows, common->plan_width);
		shared->startup_cost = matpath.startup_cost;
		shared->total_cost = matpath.total_cost;
		shared->plan_rows = common->plan_rows;
		shared->plan_width = common->plan_width;
		shared->dispatch = common->dispatch;
		shared->flow = copyObject(common->flow); 

		stype = xslice ? SHARE_MATERIAL_XSLICE : SHARE_MATERIAL; 
		m->share_id = SHARE_ID_NOT_ASSIGNED;
		m->share_type = stype;
	}

	for(i=0; i<numpartners; ++i)
	{
		Plan *p = (Plan *) make_shareinputscan(root, shared);
		shared_nodes = lappend(shared_nodes, p);
	}

	return shared_nodes;
}

/*
 * Return the total cost of sharing common numpartner times.
 * If the planner need to make a decision whether the common subplan should be shared
 * or should be duplicated, planner should compare the cost returned by this function 
 * against common->total_cost * numpartners.
 */
Cost cost_share_plan(Plan *common, PlannerInfo *root, int numpartners)
{
	Path sipath;
	Path mapath;

	if(IsA(common, Material) || IsA(common, Sort))
	{
		cost_shareinputscan(&sipath, root, common->total_cost, common->plan_rows, common->plan_width);

		return common->total_cost + (sipath.total_cost - common->total_cost) * numpartners;
	}

	cost_material(&mapath, root, common->total_cost, common->plan_rows, common->plan_width);
	cost_shareinputscan(&sipath, root, mapath.total_cost, common->plan_rows, common->plan_width);

	return mapath.total_cost + (sipath.total_cost - mapath.total_cost) * numpartners;
}

