#include "ao_reader.h"
#include "tuplebatch.h"
#include "utils/datum.h"


void
BeginVScanAppendOnlyRelation(ScanState *scanState)
{
    BeginScanAppendOnlyRelation(scanState);
    VectorizedState* vs = (VectorizedState*)scanState->ps.vectorized;
    TupleBatch tb = scanState->ss_ScanTupleSlot->PRIVATE_tb;
    vs->proj = palloc0(sizeof(bool) * tb->ncols);
    GetNeededColumnsForScan((Node* )scanState->ps.plan->targetlist,vs->proj,tb->ncols);
    GetNeededColumnsForScan((Node* )scanState->ps.plan->qual,vs->proj,tb->ncols);

}

void
EndVScanAppendOnlyRelation(ScanState *scanState)
{
    VectorizedState* vs = (VectorizedState*)scanState->ps.vectorized;
    pfree(vs->proj);
    EndScanAppendOnlyRelation(scanState);
}

TupleTableSlot *
AppendOnlyVScanNext(ScanState *scanState)
{
    TupleTableSlot *slot = scanState->ss_ScanTupleSlot;
    TupleBatch tb = (TupleBatch)slot->PRIVATE_tb;
    TupleDesc td = scanState->ss_ScanTupleSlot->tts_tupleDescriptor;
    VectorizedState* vs = scanState->ps.vectorized;
    int row = 0;

    for(;row < tb->batchsize;row ++)
    {
        AppendOnlyScanNext(scanState);

        slot = scanState->ss_ScanTupleSlot;
        if(TupIsNull(slot))
            break;

        for(int i = 0;i < tb->ncols ; i ++)
        {
           if(vs->proj[i])
            {
                Oid hawqTypeID = slot->tts_tupleDescriptor->attrs[i]->atttypid;
                Oid hawqVTypeID = GetVtype(hawqTypeID);
                if(!tb->datagroup[i])
                    tbCreateColumn(tb,i,hawqVTypeID);

                Datum *ptr = GetVFunc(hawqVTypeID)->gettypeptr(tb->datagroup[i],row);
                *ptr = slot_getattr(slot,i + 1, &(tb->datagroup[i]->isnull[row]));

                /* if attribute is a reference, deep copy the data out to prevent ao table buffer free before vectorized scan batch done */
                if(!slot->tts_mt_bind->tupdesc->attrs[i]->attbyval)
                    *ptr = datumCopy(*ptr,slot->tts_mt_bind->tupdesc->attrs[i]->attbyval,slot->tts_mt_bind->tupdesc->attrs[i]->attlen);
            }
        }

        AppendOnlyScanDesc scanDesc = ((AppendOnlyScanState*)scanState)->aos_ScanDesc;
        VarBlockHeader *header = scanDesc->executorReadBlock.varBlockReader.header;

        if (scanDesc->aos_splits_processed == list_length(scanDesc->splits) &&
            scanDesc->executorReadBlock.currentItemCount == scanDesc->executorReadBlock.readerItemCount)
        {
            if (scanDesc->executorReadBlock.varBlockReader.nextIndex >= VarBlockGet_itemCount(header))
                break;
        }
    }
    tb->nrows = row == tb->batchsize ? row : row + 1;
    TupSetVirtualTupleNValid(slot, tb->ncols);
    return slot;
}

