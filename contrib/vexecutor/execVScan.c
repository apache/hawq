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

#include "execVScan.h"
#include "miscadmin.h"
#include "execVQual.h"
#include "parquet_reader.h"
#include "ao_reader.h"

static TupleTableSlot*
ExecVScan(ScanState *node, ExecScanAccessMtd accessMtd);

static const ScanMethod *
getVScanMethod(int tableType)
{
    static const ScanMethod scanMethods[] =
            {
                    //HEAPSCAN
                    {
                    },
                    //APPENDONLYSCAN
                    {
                            &AppendOnlyVScanNext, &BeginVScanAppendOnlyRelation, &EndVScanAppendOnlyRelation,
                            &ReScanAppendOnlyRelation, &MarkRestrNotAllowed, &MarkRestrNotAllowed
                    },
                    //PARQUETSCAN
                    {
                            &ParquetVScanNext, &BeginScanParquetRelation, &EndScanParquetRelation,
                            &ReScanParquetRelation, &MarkRestrNotAllowed, &MarkRestrNotAllowed
                    }
            };

    COMPILE_ASSERT(ARRAY_SIZE(scanMethods) == TableTypeInvalid);

    if (tableType < 0 && tableType >= TableTypeInvalid)
    {
        return NULL;
    }

    return &scanMethods[tableType];
}


/*
 * ExecTableVScanVirtualLayer
 *          translate a batch of tuple to single one if scan parent is normal execution node
 *
 * VirtualNodeProc may not fill tuple data in slot success, due to qualification tag.
 * invoke scan table functoin if no tuple can pop up in TupleBatch
 */
TupleTableSlot *ExecTableVScanVirtualLayer(ScanState *scanState)
{
    VectorizedState* vs = (VectorizedState*)scanState->ps.vectorized;

    /* if vs->parent is NULL, it represent that the parent is non-vectorized */
    if(vs->parent && vs->parent->vectorized &&
       ((VectorizedState*)vs->parent->vectorized)->vectorized)
        return ExecTableVScan(scanState);
    else
    {
        TupleTableSlot* slot = scanState->ps.ps_ProjInfo ? scanState->ps.ps_ResultTupleSlot : scanState->ss_ScanTupleSlot;
        while(1)
        {
            bool succ = VirtualNodeProc(slot);

            if(!succ)
            {
                slot = ExecTableVScan(scanState);
                if(TupIsNull(slot))
                    break;
                else
                    continue;
            }

            break;
        }

        return slot;
    }
}

TupleTableSlot *ExecTableVScan(ScanState *scanState)
{
    if (scanState->scan_state == SCAN_INIT ||
        scanState->scan_state == SCAN_DONE)
    {
        getVScanMethod(scanState->tableType)->beginScanMethod(scanState);
    }

    tbReset(scanState->ss_ScanTupleSlot->PRIVATE_tb);
    tbReset(scanState->ps.ps_ResultTupleSlot->PRIVATE_tb);
    TupleTableSlot *slot = ExecVScan(scanState,getVScanMethod(scanState->tableType)->accessMethod);

    if (TupIsNull(slot) && !scanState->ps.delayEagerFree)
    {
        getVScanMethod(scanState->tableType)->endScanMethod(scanState);
    }

    return slot;

}

TupleTableSlot*
ExecVScan(ScanState *node, ExecScanAccessMtd accessMtd)
{
    ExprContext *econtext;
    List	   *qual;
    ProjectionInfo *projInfo;
    vbool *skip = NULL;

    /*
     * Fetch data from node
     */
    qual = node->ps.qual;
    projInfo = node->ps.ps_ProjInfo;

    /*
     * If we have neither a qual to check nor a projection to do, just skip
     * all the overhead and return the raw scan tuple.
     */
    if (!qual && !projInfo)
        return (*accessMtd) (node);

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    econtext = node->ps.ps_ExprContext;

    ResetExprContext(econtext);

    /*
     * get a tuple from the access method loop until we obtain a tuple which
     * passes the qualification.
     */
    for (;;)
    {
        TupleTableSlot *slot;

        CHECK_FOR_INTERRUPTS();

        slot = (*accessMtd) (node);

        /*
         * if the slot returned by the accessMtd contains NULL, then it means
         * there is nothing more to scan so we just return an empty slot,
         * being careful to use the projection result slot so it has correct
         * tupleDesc.
         */
        if (TupIsNull(slot))
        {
            if (projInfo)
                return ExecClearTuple(projInfo->pi_slot);
            else
                return slot;
        }

        /*
         * place the current tuple into the expr context
         */
        econtext->ecxt_scantuple = slot;

        /*
         * check that the current tuple satisfies the qual-clause
         *
         * check for non-nil qual here to avoid a function call to ExecQual()
         * when the qual is nil ... saves only a few cycles, but they add up
         * ...
         */
        if (!qual || (NULL != (skip = ExecVQual(qual, econtext, false))))
        {
            /*
             * Found a satisfactory scan tuple.
             */
            if (projInfo)
            {
                int i;

                /* first construct the skip array */
                if(NULL != skip)
                {
                    for(i = 0; i < skip->dim; i++)
                    {
                        ((TupleBatch)projInfo->pi_slot->PRIVATE_tb)->skip[i] =
                                          ((!DatumGetBool(skip->values[i])) ||
                                          (skip->isnull[i]) ||
                                          ((TupleBatch)slot->PRIVATE_tb)->skip[i]);
                    }
                }
                else
                {
                    Assert(((TupleBatch)projInfo->pi_slot->PRIVATE_tb)->batchsize ==
                           ((TupleBatch)slot->PRIVATE_tb)->batchsize);

                    memcpy(((TupleBatch)projInfo->pi_slot->PRIVATE_tb)->skip,
                            ((TupleBatch)slot->PRIVATE_tb)->skip,
                            ((TupleBatch)slot->PRIVATE_tb)->batchsize * sizeof(bool));
                }

                /*
                 * Form a projection tuple, store it in the result tuple slot
                 * and return it.
                 */
                ((TupleBatch)projInfo->pi_slot->PRIVATE_tb)->nrows = ((TupleBatch)slot->PRIVATE_tb)->nrows;
                return ExecVProject(projInfo, NULL);
            }
            else
            {
                /*
                 * Here, we aren't projecting, so just return scan tuple.
                 */
                return slot;
            }
        }

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);
        tbReset(node->ss_ScanTupleSlot->PRIVATE_tb);
        tbReset(node->ps.ps_ResultTupleSlot->PRIVATE_tb);
    }
}
