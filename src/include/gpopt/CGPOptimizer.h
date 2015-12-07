//---------------------------------------------------------------------------
//	@filename:
//		CGPOptimizer.h
//
//	@doc:
//		Entry point to GP optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef CGPOptimizer_H
#define CGPOptimizer_H

#include "postgres.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"

class CGPOptimizer
{
	private:

		// touch library initializers to enforce linker to include them
		static
		void TouchLibraryInitializers();

	public:

		// optimize given query using GP optimizer
		static
		PlannedStmt *PplstmtOptimize
			(
			Query *pquery,
			bool *pfUnexpectedFailure // output : set to true if optimizer unexpectedly failed to produce plan
			);

		// serialize planned statement into DXL
		static
		char *SzDXLPlan(Query *pquery);
};

#endif // CGPOptimizer_H
