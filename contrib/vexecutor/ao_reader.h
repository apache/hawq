#ifndef __AO_READER__
#define __AO_READER__

#include "postgres.h"

#include "access/heapam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/nodeAppendOnlyscan.h"

void
BeginVScanAppendOnlyRelation(ScanState *scanState);
TupleTableSlot *AppendOnlyVScanNext(ScanState *node);
void
EndVScanAppendOnlyRelation(ScanState *scanState);

#endif
