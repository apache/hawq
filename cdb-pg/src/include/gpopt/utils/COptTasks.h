//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Greenplum, Inc.
//
//	@filename:
//		COptTasks.h
//
//	@doc:
//		Tasks that will perform optimization and related tasks
//
//	@owner:
//		raghav
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef COptTasks_H
#define COptTasks_H

#include "gpos/error/CException.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/search/CSearchStage.h"



// fwd decl
namespace gpos
{
	class IMemoryPool;
	class CBitSet;
}

namespace gpdxl
{
	class CDXLNode;
}

namespace gpopt
{
	class CExpression;
	class CMDAccessor;
	class CQueryContext;
	class COptimizerConfig;
}

struct PlannedStmt;
struct Query;
struct List;

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

class COptTasks
{

	private:

		// execute a task given the argument
		static void Execute ( void *(*pfunc) (void *), void *pfuncArg);

		// routines related to serialization of pstmt and query to DXL and back
		struct SOptContext
		{
			CHAR *szQueryDXL;
			Query *pquery;
			CHAR *szPlanDXL;
			PlannedStmt *pplstmt;
			BOOL fGeneratePlStmt;
			BOOL fUnexpectedFailure;
			CHAR *szErrorMsg;
		};

		static void* PvDXLFromPlstmtTask(void *pv);
		static void* PvPlstmtFromDXLTask(void *pv);

		static void* PvDXLFromQueryTask(void *pv);
		static void* PvQueryFromDXLTask(void *pv);

		// routines relating to serialization of catalog objects to DXL
		struct SContextRelcacheToDXL
		{
			List *plOids;
			ULONG ulCmpt;		// comparison type for tasks retriving scalar comparisons
			const char *szFilename; // if filename is not null, then output will be written to file
			char *szDXL;	  // if filename is null, then output will be stored here
		};

		// dump relcache info for an object into DXL
		static
		void* PvDXLFromMDObjsTask(void *pv);

		// dump metadata about cast objects from relcache to a string in DXL format
		static
		void *PvMDCast(void *pv);
		
		// dump metadata about scalar comparison objects from relcache to a string in DXL format
		static
		void *PvMDScCmp(void *pv);
		
		// dump relstats info for an object into DXL
		static
		void* PvDXLFromRelStatsTask(void *pv);

		static
		COptimizerConfig *PoconfCreate(IMemoryPool *pmp);

		// optimize a query to a physical DXL
		static
		void* PvOptimizeTask(void *pv);

		// translate a DXL tree into a planned statement
		static
		PlannedStmt *Pplstmt(IMemoryPool *pmp, CMDAccessor *pmda, const CDXLNode *pdxln);

		// load search strategy from given path
		static
		DrgPss *PdrgPssLoad(IMemoryPool *pmp, char *szPath);

		// helper for converting wide character string to regular string
		static
		CHAR *SzFromWsz(const WCHAR *wsz);

		// lookup given exception type in the given array
		static
		BOOL FExceptionFound(gpos::CException &exc, const ULONG *pulExceptions, ULONG ulSize);

		// check if given exception is an unexpected reason for failing to produce a plan
		static
		BOOL FUnexpectedFailure(gpos::CException &exc);

		// check if given exception should error out
		static
		BOOL FErrorOut(gpos::CException &exc);

	public:

		// convert Query->DXL->LExpr->Optimize->PExpr->DXL
		static
		char *SzOptimize(Query *pquery);

		// optimize Query->DXL->LExpr->Optimize->PExpr->DXL->PlannedStmt
		static
		PlannedStmt *PplstmtOptimize
			(
			Query *pquery,
			BOOL *pfUnexpectedFailure // output : set to true if optimizer unexpectedly failed to produce plan
			);

		// convert query to DXL to xml string.
		static
		char *SzDXL(Query *pquery);

		// convert planned statement to DXL to xml string.
		static
		char *SzDXL(PlannedStmt *pplstmt);

		// convert xml string to DXL and to Query
		static
		Query *PqueryFromXML(char *szXmlString);

		// convert xml string to DXL and to PS
		static
		PlannedStmt *PplstmtFromXML(char *szXmlString);

		// dump metadata objects from relcache to file in DXL format
		static
		void DumpMDObjs(List *oids, const char *szFilename);

		// dump metadata objects from relcache to a string in DXL format
		static
		char *SzMDObjs(List *oids);
		
		// dump cast function from relcache to a string in DXL format
		static
		char *SzMDCast(List *oids);
		
		// dump scalar comparison from relcache to a string in DXL format
		static
		char *SzMDScCmp(List *oids, char *szCmpType);

		// dump statistics from relcache to a string in DXL format
		static
		char *SzRelStats(List *oids);

		// enable/disable a given xforms
		static
		bool FSetXform(char *szXform, bool fDisable);
		
		// return comparison type code
		static
		ULONG UlCmpt(char *szCmpType);

};

#endif // COptTasks_H

// EOF
