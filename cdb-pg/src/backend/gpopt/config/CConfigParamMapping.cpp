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
#include "gpopt/xforms/CXform.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

// array mapping GUCs to traceflags
CConfigParamMapping::SConfigMappingElem CConfigParamMapping::m_elem[] =
{
		{
		EopttracePrintQuery,
		&optimizer_print_query,
		GPOS_WSZ_LIT("Prints the optimizer's input query expression tree.")
		},

		{
		EopttracePrintPlan,
		&optimizer_print_plan,
		GPOS_WSZ_LIT("Prints the plan expression tree produced by the optimizer.")
		},

		{
		EopttracePrintXform,
		&optimizer_print_xform,
		GPOS_WSZ_LIT("Prints the input and output expression trees of the optimizer transformations.")
		},

		{
		EopttraceDisablePrintXformRes,
		&optimizer_disable_xform_result_printing,
		GPOS_WSZ_LIT("Disable printing input and output of xforms.")
		},

		{
		EopttracePrintMemoExplrd,
		&optimizer_print_memo_after_exploration,
		GPOS_WSZ_LIT("Prints MEMO after exploration.")
		},

		{
		EopttracePrintMemoImpld,
		&optimizer_print_memo_after_implementation,
		GPOS_WSZ_LIT("Prints MEMO after implementation.")
		},

		{
		EopttracePrintMemoOptd,
		&optimizer_print_memo_after_optimization,
		GPOS_WSZ_LIT("Prints MEMO after optimization.")
		},

		{
		EopttracePrintScheduler,
		&optimizer_print_job_scheduler,
		GPOS_WSZ_LIT("Prints jobs in scheduler on each job completion.")
		},

		{
		EopttracePrintExprProps,
		&optimizer_print_expression_properties,
		GPOS_WSZ_LIT("Prints expression properties.")
		},

		{
		EopttracePrintGrpProps,
		&optimizer_print_group_properties,
		GPOS_WSZ_LIT("Prints group properties.")
		},

		{
		EopttracePrintOptCtxt,
		&optimizer_print_optimization_context,
		GPOS_WSZ_LIT("Prints optimization context.")
		},

		{
		EopttracePrintOptStats,
		&optimizer_print_optimization_stats,
		GPOS_WSZ_LIT("Prints optimization stats.")
		},

		{
		EopttraceParallel,
		&optimizer_parallel,
		GPOS_WSZ_LIT("Enable using threads in optimization engine.")
		},

		{
		EopttraceMinidump,
		&optimizer_minidump,
		GPOS_WSZ_LIT("Generate optimizer minidump.")
		},

		{
		EopttraceExtractDXLStats,
		&optimizer_extract_dxl_stats,
		GPOS_WSZ_LIT("Extract plan stats in dxl.")
		},

		{
		EopttraceExtractDXLStatsAllNodes,
		&optimizer_extract_dxl_stats_all_nodes,
		GPOS_WSZ_LIT("Extract plan stats for all physical dxl nodes.")
		},

		{
                EopttraceDeriveStatsForDPE,
		&optimizer_dpe_stats,
                GPOS_WSZ_LIT("Enable stats derivation of partitioned tables with dynamic partition elimination.")
                },

		{
		EopttraceEnumeratePlans,
		&optimizer_enumerate_plans,
		GPOS_WSZ_LIT("Enable plan enumeration.")
		},

		{
		EopttraceSamplePlans,
		&optimizer_sample_plans,
		GPOS_WSZ_LIT("Enable plan sampling.")
		},

		{
		EopttraceEnableCTEInlining,
		&optimizer_cte_inlining,
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

		if (optimizer_xforms[ul])
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif // GPOS_DEBUG
				pbs->FExchangeSet(EopttraceDisableXformBase + ul);
			GPOS_ASSERT(!fSet);
		}
	}

	// disable index-join if the corresponding GUC is turned off
	if (!optimizer_indexjoin)
	{
		(void) pbs->FExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2IndexApply));
	}

	return pbs;
}

// EOF
