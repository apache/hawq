//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConfigParamMapping.cpp
//
//	@doc:
//		Implementation of GPDB config params->trace flags mapping
//
//	@owner:
//		antovl
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "utils/guc.h"

#include "gpopt/config/CConfigParamMapping.h"

using namespace gpos;
using namespace gpdxl;

// array mapping GUCs to traceflags
CConfigParamMapping::SConfigMappingElem CConfigParamMapping::m_elem[] =
{
		{
		EopttracePrintQuery,
		&gp_opt_print_query,
		GPOS_WSZ_LIT("Prints the optimizer's input query expression tree.")
		},

		{
		EopttracePrintPlan,
		&gp_opt_print_plan,
		GPOS_WSZ_LIT("Prints the plan expression tree produced by the optimizer.")
		},

		{
		EopttracePrintXform,
		&gp_opt_print_xform,
		GPOS_WSZ_LIT("Prints the input and output expression trees of the optimizer transformations.")
		},

		{
		EopttraceDisablePrintXformRes,
		&gp_opt_disable_xform_result_printing,
		GPOS_WSZ_LIT("Disable printing input and output of xforms.")
		},

		{
		EopttracePrintMemoExplrd,
		&gp_opt_print_memo_after_exploration,
		GPOS_WSZ_LIT("Prints MEMO after exploration.")
		},

		{
		EopttracePrintMemoImpld,
		&gp_opt_print_memo_after_implementation,
		GPOS_WSZ_LIT("Prints MEMO after implementation.")
		},

		{
		EopttracePrintMemoOptd,
		&gp_opt_print_memo_after_optimization,
		GPOS_WSZ_LIT("Prints MEMO after optimization.")
		},

		{
		EopttracePrintScheduler,
		&gp_opt_print_job_scheduler,
		GPOS_WSZ_LIT("Prints jobs in scheduler on each job completion.")
		},

		{
		EopttracePrintExprProps,
		&gp_opt_print_expression_properties,
		GPOS_WSZ_LIT("Prints expression properties.")
		},

		{
		EopttracePrintGrpProps,
		&gp_opt_print_group_properties,
		GPOS_WSZ_LIT("Prints group properties.")
		},

		{
		EopttracePrintOptCtxt,
		&gp_opt_print_optimization_context,
		GPOS_WSZ_LIT("Prints optimization context.")
		},

		{
		EopttracePrintOptStats,
		&gp_opt_print_optimization_stats,
		GPOS_WSZ_LIT("Prints optimization stats.")
		},

		{
		EopttraceParallel,
		&gp_opt_parallel,
		GPOS_WSZ_LIT("Enable using threads in optimization engine.")
		},

		{
		EopttraceMinidump,
		&gp_opt_minidump,
		GPOS_WSZ_LIT("Produce a minidump on every optimization.")
		},

		{
		EopttraceExtractDXLStats,
		&gp_opt_extract_dxl_stats,
		GPOS_WSZ_LIT("Extract plan stats in dxl.")
		},

		{
		EopttraceExtractDXLStatsAllNodes,
		&gp_opt_extract_dxl_stats_all_nodes,
		GPOS_WSZ_LIT("Extract plan stats for all physical dxl nodes.")
		},

		{
		EopttraceEnumeratePlans,
		&gp_opt_enumerate_plans,
		GPOS_WSZ_LIT("Enable plan enumeration.")
		},

		{
		EopttraceSamplePlans,
		&gp_opt_sample_plans,
		GPOS_WSZ_LIT("Enable plan sampling.")
		},

		{
		EopttraceEnableCTEInlining,
		&gp_opt_cte_inlining,
		GPOS_WSZ_LIT("Enable CTE inlining.")
		},

};

//---------------------------------------------------------------------------
//	@function:
//		CConfigParamMapping::PbsPack
//
//	@doc:
//		Pack the GPDB config params into a bitset
//
//---------------------------------------------------------------------------
CBitSet *
CConfigParamMapping::PbsPack
	(
	IMemoryPool *pmp,
	ULONG ulXforms // number of available xforms
	)
{
	CBitSet *pbs = New(pmp) CBitSet(pmp, EopttraceSentinel);

	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(m_elem); ul++)
	{
		SConfigMappingElem elem = m_elem[ul];
		GPOS_ASSERT(!pbs->FBit((ULONG) elem.m_etf) &&
					"trace flag already set");

		if (*elem.m_pfParam)
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif // GPOS_DEBUG
				pbs->FExchangeSet((ULONG) elem.m_etf);
			GPOS_ASSERT(!fSet);
		}
	}

	// pack disable flags of xforms
	for (ULONG ul = 0; ul < ulXforms; ul++)
	{
		GPOS_ASSERT(!pbs->FBit(EopttraceDisableXformBase + ul) &&
					"xform trace flag already set");

		if (gp_opt_xforms[ul])
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif // GPOS_DEBUG
				pbs->FExchangeSet(EopttraceDisableXformBase + ul);
			GPOS_ASSERT(!fSet);
		}
	}

	return pbs;
}

// EOF
