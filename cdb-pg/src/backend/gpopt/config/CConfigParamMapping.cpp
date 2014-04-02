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
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints the optimizer's input query expression tree.")
		},

		{
		EopttracePrintPlan,
		&optimizer_print_plan,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints the plan expression tree produced by the optimizer.")
		},

		{
		EopttracePrintXform,
		&optimizer_print_xform,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints the input and output expression trees of the optimizer transformations.")
		},

		{
		EopttraceDisablePrintXformRes,
		&optimizer_disable_xform_result_printing,
		false, // m_fNegate
		GPOS_WSZ_LIT("Disable printing input and output of xforms.")
		},

		{
		EopttracePrintMemoExplrd,
		&optimizer_print_memo_after_exploration,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints MEMO after exploration.")
		},

		{
		EopttracePrintMemoImpld,
		&optimizer_print_memo_after_implementation,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints MEMO after implementation.")
		},

		{
		EopttracePrintMemoOptd,
		&optimizer_print_memo_after_optimization,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints MEMO after optimization.")
		},

		{
		EopttracePrintScheduler,
		&optimizer_print_job_scheduler,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints jobs in scheduler on each job completion.")
		},

		{
		EopttracePrintExprProps,
		&optimizer_print_expression_properties,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints expression properties.")
		},

		{
		EopttracePrintGrpProps,
		&optimizer_print_group_properties,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints group properties.")
		},

		{
		EopttracePrintOptCtxt,
		&optimizer_print_optimization_context,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints optimization context.")
		},

		{
		EopttracePrintOptStats,
		&optimizer_print_optimization_stats,
		false, // m_fNegate
		GPOS_WSZ_LIT("Prints optimization stats.")
		},

		{
		EopttraceParallel,
		&optimizer_parallel,
		false, // m_fNegate
		GPOS_WSZ_LIT("Enable using threads in optimization engine.")
		},

		{
		EopttraceMinidump,
		&optimizer_minidump,
		false, // m_fNegate
		GPOS_WSZ_LIT("Generate optimizer minidump.")
		},
             	
		{
                EopttraceDisableMotions,
                &optimizer_enable_motions,
                true, // m_fNegate
		GPOS_WSZ_LIT("Disable motion nodes in optimizer.")
                },

                {
                EopttraceDisableMotionBroadcast,
                &optimizer_enable_motion_broadcast,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable motion broadcast nodes in optimizer.")
                },

                {
                EopttraceDisableMotionGather,
                &optimizer_enable_motion_gather,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable motion gather nodes in optimizer.")
                },

                {
                EopttraceDisableMotionHashDistribute,
                &optimizer_enable_motion_redistribute,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable motion hash-distribute nodes in optimizer.")
                },

                {
                EopttraceDisableMotionRandom,
                &optimizer_enable_motion_redistribute,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable motion random nodes in optimizer.")
                },

                {
                EopttraceDisableMotionRountedDistribute,
                &optimizer_enable_motion_redistribute,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable motion routed-distribute nodes in optimizer.")
                },

                {
                EopttraceDisableSort,
                &optimizer_enable_sort,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable sort nodes in optimizer.")
                },

                {
                EopttraceDisableSpool,
                &optimizer_enable_materialize,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable spool nodes in optimizer.")
                },

                {
                EopttraceDisablePartPropagation,
                &optimizer_enable_partition_propagation,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable partition propagation nodes in optimizer.")
                },
                
                {
                EopttraceDisablePartSelection,
                &optimizer_enable_partition_selection,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable partition selection in optimizer.")
                },

                {
                EopttraceDisableOuterJoin2InnerJoinRewrite,
                &optimizer_enable_outerjoin_rewrite,
                true, // m_fNegate
                GPOS_WSZ_LIT("Disable outer join to inner join rewrite in optimizer.")
                },

		{
		EopttraceExtractDXLStats,
		&optimizer_extract_dxl_stats,
		false, // m_fNegate
		GPOS_WSZ_LIT("Extract plan stats in dxl.")
		},

		{
		EopttraceExtractDXLStatsAllNodes,
		&optimizer_extract_dxl_stats_all_nodes,
		false, // m_fNegate
		GPOS_WSZ_LIT("Extract plan stats for all physical dxl nodes.")
		},

		{
                EopttraceDeriveStatsForDPE,
		&optimizer_dpe_stats,
                false, // m_fNegate
		GPOS_WSZ_LIT("Enable stats derivation of partitioned tables with dynamic partition elimination.")
                },

		{
		EopttraceEnumeratePlans,
		&optimizer_enumerate_plans,
		false, // m_fNegate
		GPOS_WSZ_LIT("Enable plan enumeration.")
		},

		{
		EopttraceSamplePlans,
		&optimizer_sample_plans,
		false, // m_fNegate
		GPOS_WSZ_LIT("Enable plan sampling.")
		},

		{
		EopttraceEnableCTEInlining,
		&optimizer_cte_inlining,
		false, // m_fNegate
		GPOS_WSZ_LIT("Enable CTE inlining.")
		},

		{
		EopttraceEnableConstantExpressionEvaluation,
		&optimizer_enable_constant_expression_evaluation,
		false,  // m_fNegate
		GPOS_WSZ_LIT("Enable constant expression evaluation in the optimizer")
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

		BOOL fVal = *elem.m_pfParam;
		if (elem.m_fNegate)
		{
			// negate the value of config param
			fVal = !fVal;
		}

		if (fVal)
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
	if (!optimizer_enable_indexjoin)
	{
		(void) pbs->FExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2IndexGetApply));
		(void) pbs->FExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2DynamicIndexGetApply));
	}

	return pbs;
}

// EOF
