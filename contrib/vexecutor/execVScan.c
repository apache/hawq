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
                            &AppendOnlyScanNext, &BeginScanAppendOnlyRelation, &EndScanAppendOnlyRelation,
                            &ReScanAppendOnlyRelation, &MarkRestrNotAllowed, &MarkRestrNotAllowed
                    },
                    //PARQUETSCAN
                    {
                            &ParquetScanNext, &BeginScanParquetRelation, &EndScanParquetRelation,
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

TupleTableSlot *ExecTableVScan(ScanState *scanState)
{
    if (scanState->scan_state == SCAN_INIT ||
        scanState->scan_state == SCAN_DONE)
    {
        getVScanMethod(scanState->tableType)->beginScanMethod(scanState);
    }

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
        if (!qual || ExecQual(qual, econtext, false))
        {
            /*
             * Found a satisfactory scan tuple.
             */
            if (projInfo)
            {
                /*
                 * Form a projection tuple, store it in the result tuple slot
                 * and return it.
                 */
                return ExecProject(projInfo, NULL);
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
    }
}