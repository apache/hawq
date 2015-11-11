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

/*
 * execGpmon.c
 *   Gpmon related functions inside executor.
 *
 */
#include "postgres.h"

#include "executor/executor.h"
#include "utils/lsyscache.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"

char * GetScanRelNameGpmon(Oid relid, char schema_rel_name[SCAN_REL_NAME_BUF_SIZE])
{
	if (relid > 0)
	{
		char *relname = get_rel_name(relid);
		char *schemaname = get_namespace_name(get_rel_namespace(relid));
		snprintf(schema_rel_name, SCAN_REL_NAME_BUF_SIZE, "%s.%s", schemaname, relname);
		if (relname)
		{
			pfree(relname);
		}
		
		if (schemaname)
		{
			pfree(schemaname);
		}
	}
	return schema_rel_name;
}

void CheckSendPlanStateGpmonPkt(PlanState *ps)
{
	if(!ps)
		return;

	if(gp_enable_gpperfmon)
	{
		if(!ps->fHadSentGpmon || ps->gpmon_plan_tick != gpmon_tick)
		{
			if(ps->state && LocallyExecutingSliceIndex(ps->state) == currentSliceId)
			{
				gpmon_send(&ps->gpmon_pkt);
			}
			
			ps->fHadSentGpmon = true;
		}

		ps->gpmon_plan_tick = gpmon_tick;
	}
}

void EndPlanStateGpmonPkt(PlanState *ps)
{
	if(!ps)
		return;

	ps->gpmon_pkt.u.qexec.status = (uint8)PMNS_Finished;

	if(gp_enable_gpperfmon &&
	   ps->state &&
	   LocallyExecutingSliceIndex(ps->state) == currentSliceId)
	{
		gpmon_send(&ps->gpmon_pkt);
	}
}

/*
 * InitPlanNodeGpmonPkt -- initialize the init gpmon package, and send it off.
 */
void InitPlanNodeGpmonPkt(Plan *plan, gpmon_packet_t *gpmon_pkt, EState *estate,
						  PerfmonNodeType type,
						  int64 rowsout_est,
						  char* relname)
{
	int rowsout_adjustment_factor = 0;

	if(!plan)
		return;

	/* The estimates are now global so we need to adjust by
	 * the number of segments in the array.
	 */
	rowsout_adjustment_factor = GetQEGangNum();

	/* Make sure we don't div by zero below */
	if (rowsout_adjustment_factor < 1)
		rowsout_adjustment_factor = 1;

	Assert(rowsout_adjustment_factor >= 1);

	memset(gpmon_pkt, 0, sizeof(gpmon_packet_t));

	gpmon_pkt->magic = GPMON_MAGIC;
	gpmon_pkt->version = GPMON_PACKET_VERSION;
	gpmon_pkt->pkttype = GPMON_PKTTYPE_QEXEC;

	gpmon_gettmid(&gpmon_pkt->u.qexec.key.tmid);
	gpmon_pkt->u.qexec.key.ssid = gp_session_id;
	gpmon_pkt->u.qexec.key.ccnt = gp_command_count;
	gpmon_pkt->u.qexec.key.hash_key.segid = GetQEIndex();
	gpmon_pkt->u.qexec.key.hash_key.pid = MyProcPid;
	gpmon_pkt->u.qexec.key.hash_key.nid = plan->plan_node_id;

	gpmon_pkt->u.qexec.pnid = plan->plan_parent_node_id;


	gpmon_pkt->u.qexec.nodeType = (apr_uint16_t)type;

	gpmon_pkt->u.qexec.rowsout = 0;
	gpmon_pkt->u.qexec.rowsout_est = rowsout_est / rowsout_adjustment_factor;

	if (relname)
	{
		snprintf(gpmon_pkt->u.qexec.relation_name, sizeof(gpmon_pkt->u.qexec.relation_name), "%s", relname);
	}

	gpmon_pkt->u.qexec.status = (uint8)PMNS_Initialize;

	if(gp_enable_gpperfmon && estate)
	{
		gpmon_send(gpmon_pkt);
	}

	gpmon_pkt->u.qexec.status = (uint8)PMNS_Executing;
}
