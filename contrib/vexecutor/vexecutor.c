#include "vexecutor.h"
#include "c.h"
#include "executor/executor.h"
//#include "executor/execHHashagg.h"
//#include "executor/nodeAgg.h"

PG_MODULE_MAGIC;

/*
 * hook function
 */
static PlanState* VExecInitNode(Plan *node,EState *eState,int eflags);
static TupleTableSlot* VExecProcNode(PlanState *node);
static bool VExecEndNode(PlanState *node);

/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void
_PG_init(void)
{
	elog(DEBUG3, "PG INIT VEXECTOR");
    vmthd.ExecInitNode_H = VExecInitNode;
	vmthd.ExecProcNode_H = VExecProcNode;
	vmthd.ExecEndNode_H = VExecEndNode;
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
	elog(DEBUG3, "PG FINI VEXECTOR");
	vmthd.ExecInitNode_H = NULL;
	vmthd.ExecProcNode_H = NULL;
	vmthd.ExecEndNode_H = NULL;

}

static PlanState* VExecInitNode(Plan *node,EState *eState,int eflags)
{
	elog(DEBUG3, "PG VEXEC INIT NODE");
	return NULL;
}
static TupleTableSlot* VExecProcNode(PlanState *node)
{
	return NULL;
}

static bool VExecEndNode(PlanState *node)
{
	elog(DEBUG3, "PG VEXEC END NODE");
	return false;
}
