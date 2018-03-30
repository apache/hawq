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
#include "execVQual.h"

/*
 * ExecVariableList
 *		Evaluates a simple-Variable-list projection.
 *
 * Results are stored into the passed values and isnull arrays.
 */
static void
ExecVecVariableList(ProjectionInfo *projInfo,
                 Datum *values)
{
    ExprContext *econtext = projInfo->pi_exprContext;
    int		   *varSlotOffsets = projInfo->pi_varSlotOffsets;
    int		   *varNumbers = projInfo->pi_varNumbers;
    TupleBatch  tb = (TupleBatch) values;
    int			i;
    tb->ncols = list_length(projInfo->pi_targetlist);

    /*
     * Assign to result by direct extraction of fields from source slots ... a
     * mite ugly, but fast ...
     */
    for (i = list_length(projInfo->pi_targetlist) - 1; i >= 0; i--)
    {
        char	   *slotptr = ((char *) econtext) + varSlotOffsets[i];
        TupleTableSlot *varSlot = *((TupleTableSlot **) slotptr);
        int			varNumber = varNumbers[i] - 1;
        tb->datagroup[i] = ((TupleBatch)varSlot->PRIVATE_tb)->datagroup[varNumber];
    }
}

TupleTableSlot *
ExecVProject(ProjectionInfo *projInfo, ExprDoneCond *isDone)
{
    TupleTableSlot *slot;
    Assert(projInfo != NULL);

    /*
     * get the projection info we want
     */
    slot = projInfo->pi_slot;

    /*
     * Clear any former contents of the result slot.  This makes it safe for
     * us to use the slot's Datum/isnull arrays as workspace. (Also, we can
     * return the slot as-is if we decide no rows can be projected.)
     */
    ExecClearTuple(slot);

    /*
     * form a new result tuple (if possible); if successful, mark the result
     * slot as containing a valid virtual tuple
     */
    if (projInfo->pi_isVarList)
    {
        /* simple Var list: this always succeeds with one result row */
        if (isDone)
            *isDone = ExprSingleResult;

        ExecVecVariableList(projInfo,slot->PRIVATE_tb);
        ExecStoreVirtualTuple(slot);
    }
    else
    {
        elog(FATAL,"does not support expression in projection stmt");
       // if (ExecTargetList(projInfo->pi_targetlist,
       //                    projInfo->pi_exprContext,
       //                    slot_get_values(slot),
       //                    slot_get_isnull(slot),
       //                    (ExprDoneCond *) projInfo->pi_itemIsDone,
       //                    isDone))
       //     ExecStoreVirtualTuple(slot);
    }

    return slot;
}

/*
 * VirtualNodeProc
 *      return value indicate whether has a tuple data fill in slot->PRIVATE_tts_values slot
 *      This function will be invoked in V->N process.
 */
bool
VirtualNodeProc(ScanState* state,TupleTableSlot *slot){
    if(TupIsNull(slot) )
        return false;

    TupleBatch tb = slot->PRIVATE_tb;
    ExecClearTuple(slot);

    while (tb->skip[tb->iter] && tb->iter < tb->nrows)
        tb->iter++;

    if(tb->iter == tb->nrows)
        return false;

    for(int i = 0;i < tb->ncols;i ++)
    {
        vheader *header = tb->datagroup[i];
        GetVFunc(GetVtype(header->elemtype))->gettypevalue(header,tb->iter,slot->PRIVATE_tts_values + i);
        slot->PRIVATE_tts_isnull[i] = header->isnull[tb->iter];
    }
    tb->iter ++;
    ExecStoreVirtualTuple(slot);
    return true;
}
