#include "ao_reader.h"
#include "tuplebatch.h"
#include "utils/datum.h"

extern  MemTuple
appendonlygettup(AppendOnlyScanDesc scan,
                 ScanDirection dir __attribute__((unused)),
                 int nkeys,
                 ScanKey key,
                 TupleTableSlot *slot);

void
BeginVScanAppendOnlyRelation(ScanState *scanState)
{
    BeginScanAppendOnlyRelation(scanState);
    VectorizedState* vs = (VectorizedState*)scanState->ps.vectorized;
    TupleBatch tb = scanState->ss_ScanTupleSlot->PRIVATE_tb;
    vs->ao = palloc0(sizeof(aoinfo));
    vs->ao->proj = palloc0(sizeof(bool) * tb->ncols);
    GetNeededColumnsForScan((Node* )scanState->ps.plan->targetlist,vs->ao->proj,tb->ncols);
    GetNeededColumnsForScan((Node* )scanState->ps.plan->qual,vs->ao->proj,tb->ncols);
}

void
EndVScanAppendOnlyRelation(ScanState *scanState)
{
    VectorizedState* vs = (VectorizedState*)scanState->ps.vectorized;
    pfree(vs->ao->proj);
    pfree(vs->ao);
    EndScanAppendOnlyRelation(scanState);
}

static TupleTableSlot *
AOScanNext(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	AppendOnlyScanState *node = (AppendOnlyScanState *)scanState;
	VectorizedState* vs = scanState->ps.vectorized;

	AppendOnlyScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	Assert((node->ss.scan_state & SCAN_SCAN) != 0);

	estate = node->ss.ps.state;
	scandesc = node->aos_ScanDesc;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	MemTuple tup = appendonlygettup(scandesc, direction, scandesc->aos_nkeys, scandesc->aos_key, slot);

	if (tup == NULL)
	{
        vs->ao->isDone = true;
        return NULL;
	}

	pgstat_count_heap_getnext(scandesc->aos_rd);

    return slot;
}


TupleTableSlot *
AppendOnlyVScanNext(ScanState *scanState)
{
    TupleTableSlot *slot = scanState->ss_ScanTupleSlot;
    TupleBatch tb = (TupleBatch)slot->PRIVATE_tb;
    VectorizedState* vs = scanState->ps.vectorized;

    if(vs->ao->isDone)
    {
        ExecClearTuple(slot);
        return slot;
    }

    for(tb->nrows = 0;tb->nrows < tb->batchsize;tb->nrows ++)
    {
        slot = AOScanNext(scanState);

        if(TupIsNull(slot))
            break;

        for(int i = 0;i < tb->ncols ; i ++)
        {
           if(vs->ao->proj[i])
            {
                Oid hawqTypeID = slot->tts_tupleDescriptor->attrs[i]->atttypid;
                Oid hawqVTypeID = GetVtype(hawqTypeID);
                if(!tb->datagroup[i])
                    tbCreateColumn(tb,i,hawqVTypeID);

                tb->datagroup[i]->values[tb->nrows] = slot_getattr(slot,i + 1, &(tb->datagroup[i]->isnull[tb->nrows]));

                /* if attribute is a reference, deep copy the data out to prevent ao table buffer free before vectorized scan batch done */
                if(!slot->tts_mt_bind->tupdesc->attrs[i]->attbyval)
                    tb->datagroup[i]->values[tb->nrows]= datumCopy(tb->datagroup[i]->values[tb->nrows],
                                                                   slot->tts_mt_bind->tupdesc->attrs[i]->attbyval,
                                                                   slot->tts_mt_bind->tupdesc->attrs[i]->attlen);

            }
        }
    }

    if(!slot)
        slot = scanState->ss_ScanTupleSlot;

    for(int i = 0;i < tb->ncols ; i ++)
    {
        if(vs->ao->proj[i])
        {
            if(tb->datagroup[i])
                tb->datagroup[i]->dim = tb->nrows;
        }
    }

    if (tb->nrows == 0)
        ExecClearTuple(slot);
    else
        TupSetVirtualTupleNValid(slot, tb->ncols);
    return slot;
}

