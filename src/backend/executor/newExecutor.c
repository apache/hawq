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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * newExecutor.c
 *	  pluggable new executor
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbexplain.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/datetime.h"
#include "utils/hawq_type_mapping.h"

#include "executor/cwrapper/executor-c.h"

void MyExecutorRun(ExecutorC *exec) __attribute__((weak));
bool enableOushuDbExtensiveFeatureSupport() {
  return MyExecutorRun != NULL;
}
void checkOushuDbExtensiveFeatureSupport(char featureCategory[]) {
  if (MyExecutorRun == NULL)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("Feature of %s is not supported in Apache "
                        "HAWQ", featureCategory)));
}

void checkOushuDbExtensiveFunctionSupport(char functionString[]) {
  if (MyExecutorRun == NULL)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("%s function is only supported in OushuDB "
                        "Enterprise Edition", functionString)));
}

PlanState *newExecutorPlanStateReference = NULL;

void exec_mpp_query_new(void *dispatchData, const char *plan, int len, int stageNo, bool setDisplay,
                        DestReceiver *dest, PlanState *planstate) {
  checkOushuDbExtensiveFeatureSupport("New Executor");
  Assert(MyNewExecutor != NULL);
  newExecutorPlanStateReference = planstate;

  const char *queryId = palloc(sizeof(int) + sizeof(int) + 5);
  sprintf(queryId, "QID%d_%d", gp_session_id, gp_command_count);
  int vsegNum = GetQEGangNum();
  int rangeNum = 0;
  ExecutorNewWorkHorse(MyNewExecutor, dispatchData, plan, len, queryId, stageNo, GetQEIndex(),
                       vsegNum, DateStyle, DateOrder, &rangeNum);
  MyExecutorSetJumpHashMap(MyNewExecutor, get_jump_hash_map(rangeNum),
                           JUMP_HASH_MAP_LENGTH);
  ExecutorCatchedError *err = ExecutorGetLastError(MyNewExecutor);

  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    int errCode = err->errCode;
    if (Gp_role == GP_ROLE_DISPATCH) ExecutorFreeWorkHorse(MyNewExecutor);
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }

  if (setDisplay) set_ps_display(MyExecutorGetCommandTag(MyNewExecutor), false);

  if (dest != NULL)
    MyExecutorSetSendResultCallBack(MyNewExecutor, dest->receiveSlotNewPlan);

  MyExecutorSetSendExecStatsCallBack(MyNewExecutor, newplan_sendExecStats);

  MyExecutorRun(MyNewExecutor);

  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    int errCode = err->errCode;
    if (Gp_role == GP_ROLE_DISPATCH) ExecutorFreeWorkHorse(MyNewExecutor);
    if (errCode != ERRCODE_QUERY_CANCELED)
      ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  } else {
    ExecutorFreeWorkHorse(MyNewExecutor);
  }
  newExecutorPlanStateReference = NULL;
}

void teardownNewInterconnect() {
  ExecutorTearDownInterconnect(MyNewExecutor);
}

MyNewExecutorTupState *makeMyNewExecutorTupState(TupleDesc tupdesc) {
  MyNewExecutorTupState *state =
      (MyNewExecutorTupState *)palloc(sizeof(MyNewExecutorTupState));
  state->slot = MakeSingleTupleTableSlot(tupdesc);
  state->hasTuple = false;

  state->numberOfColumns = tupdesc->natts;
  state->colValues = palloc0(sizeof(Datum) * state->numberOfColumns);
  state->colIsNulls = palloc0(sizeof(bool) * state->numberOfColumns);
  state->colRawValues = palloc0(sizeof(char *) * state->numberOfColumns);
  state->colValLength = palloc0(sizeof(uint64_t) * state->numberOfColumns);
  state->colValNullBitmap = palloc0(sizeof(bits8 *) * state->numberOfColumns);
  state->colAddresses = palloc0(sizeof(char *) * state->numberOfColumns);
  state->colSpeedUpPossible = palloc0(sizeof(bool) * state->numberOfColumns);
  state->colSpeedUp = palloc0(sizeof(bool) * state->numberOfColumns);
  state->reserved = 8;
  state->nulls = palloc0(sizeof(bool) * state->reserved);
  state->datums = palloc0(sizeof(Datum) * state->reserved);
  state->in_functions =
      (FmgrInfo *)palloc(sizeof(FmgrInfo) * state->numberOfColumns);
  state->typioparams = (Oid *)palloc(sizeof(Oid) * state->numberOfColumns);

  for (int i = 0; i < state->numberOfColumns; i++) {
    Oid data_type = tupdesc->attrs[i]->atttypid;
    state->colValues[i] = NULL;
    state->colIsNulls[i] = false;
    state->colRawValues[i] = NULL;
    state->colValLength[i] = 0;
    state->colValNullBitmap[i] = (bits8 *)palloc0(sizeof(bits8) * 10000);
    state->colAddresses[i] = NULL;
    state->colSpeedUp[i] = false;
    if (data_type == HAWQ_TYPE_TEXT || data_type == HAWQ_TYPE_BYTE ||
        data_type == HAWQ_TYPE_BPCHAR || data_type == HAWQ_TYPE_VARCHAR ||
        data_type == HAWQ_TYPE_NUMERIC) {
      state->colSpeedUpPossible[i] = true;
    } else {
      state->colSpeedUpPossible[i] = false;
    }
    Oid in_func_oid;
    getTypeInputInfo(data_type, &in_func_oid, &state->typioparams[i]);
    fmgr_info(in_func_oid, &state->in_functions[i]);
  }
  return state;
}

void beginMyNewExecutor(void *dispatchData, const char *plan, int len, int stageNo,
                        PlanState *planstate) {
  Assert(MyNewExecutor != NULL);
  newExecutorPlanStateReference = planstate;

  const char *queryId = palloc(sizeof(int) + sizeof(int) + 5);
  sprintf(queryId, "QID%d_%d", gp_session_id, gp_command_count);
  int vsegNum = GetQEGangNum();
  int rangeNum = 0;
  ExecutorNewWorkHorse(MyNewExecutor, dispatchData, plan, len, queryId, stageNo, GetQEIndex(),
                       vsegNum, DateStyle, DateOrder, &rangeNum);
  MyExecutorSetJumpHashMap(MyNewExecutor, get_jump_hash_map(rangeNum),
                           JUMP_HASH_MAP_LENGTH);
  ExecutorCatchedError *err = ExecutorGetLastError(MyNewExecutor);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    int errCode = err->errCode;
    if (Gp_role == GP_ROLE_DISPATCH) ExecutorFreeWorkHorse(MyNewExecutor);
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }

  MyExecutorSetSendExecStatsCallBack(MyNewExecutor, newplan_sendExecStats);

  MyExecutorBegin(MyNewExecutor);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    int errCode = err->errCode;
    if (Gp_role == GP_ROLE_DISPATCH) ExecutorFreeWorkHorse(MyNewExecutor);
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }
}

void execMyNewExecutor(MyNewExecutorTupState *newExecutorState) {
  TupleTableSlot *tts = newExecutorState->slot;
  TupleDesc tupDesc = tts->tts_tupleDescriptor;

  Oid dataType;
  /* Free memory for previous tuple if necessary */
  for (int i = 0; i < newExecutorState->numberOfColumns; ++i) {
    dataType = tupDesc->attrs[i]->atttypid;
    if (!newExecutorState
             ->colIsNulls[i] &&  // Column not to read or column is null
        ((newExecutorState
              ->colSpeedUpPossible[i] &&  // Column is TEXT or VARCHAR
          !newExecutorState->colSpeedUp[i]) ||
         dataType == HAWQ_TYPE_INT2_ARRAY || dataType == HAWQ_TYPE_INT4_ARRAY ||
         dataType == HAWQ_TYPE_INT8_ARRAY ||
         dataType == HAWQ_TYPE_FLOAT4_ARRAY ||
         dataType == HAWQ_TYPE_FLOAT8_ARRAY)) {
      char *ptr = DatumGetPointer(newExecutorState->colValues[i]);
      if (ptr) pfree(ptr);
    }
  }

  bool last_batch_row = false;
  bool res = MyExecutorGetTuple(
      MyNewExecutor, newExecutorState->colRawValues,
      newExecutorState->colValLength, newExecutorState->colValNullBitmap,
      newExecutorState->colIsNulls, newExecutorState->colAddresses,
      newExecutorState->colSpeedUpPossible, newExecutorState->colSpeedUp,
      &last_batch_row);
  ExecutorCatchedError *err = ExecutorGetLastError(MyNewExecutor);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    int errCode = err->errCode;
    if (Gp_role == GP_ROLE_DISPATCH) ExecutorFreeWorkHorse(MyNewExecutor);
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }

  if (res) {
    /* We have one tuple ready */
    for (int i = 0; i < newExecutorState->numberOfColumns; ++i) {
      if (newExecutorState
              ->colIsNulls[i])  // Column not to read or column is null
      {
        continue;
      }

      /*
       * HAWQ_TYPE_CHAR,
       * HAWQ_TYPE_INT2, HAWQ_TYPE_INT4, HAWQ_TYPE_INT8
       * HAWQ_TYPE_FLOAT4, HAWQ_TYPE_FLOAT8
       * HAWQ_TYPE_BOOL, HAWQ_TYPE_DATE, HAWQ_TYPE_TIME
       * HAWQ_TYPE_TIMESTAMP
       */
      dataType = tupDesc->attrs[i]->atttypid;
      if (!newExecutorState->colSpeedUpPossible[i]) {
        if (dataType == HAWQ_TYPE_DATE) {
          *(int32_t *)(newExecutorState->colRawValues[i]) -=
              POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
          newExecutorState->colValues[i] =
              *(Datum *)(newExecutorState->colRawValues[i]);
        } else if (dataType == HAWQ_TYPE_INT2_ARRAY) {
          int dims[1];
          int lbs[1];
          dims[0] = newExecutorState->colValLength[i];
          lbs[0] = 1;
          if (newExecutorState->reserved < dims[0]) {
            newExecutorState->nulls = (bool *)repalloc(newExecutorState->nulls,
                                                       sizeof(bool) * dims[0]);
            newExecutorState->datums = (Datum *)repalloc(
                newExecutorState->datums, sizeof(Datum) * dims[0]);
          }
          int bitmask = 1;
          for (int j = 0; j < dims[0]; j++) {
            if (bitmask == 0x100) bitmask = 1;
            newExecutorState->nulls[j] =
                newExecutorState->colValNullBitmap[i][j / 8] & bitmask ? false
                                                                       : true;
            bitmask <<= 1;
          }
          for (int j = 0; j < dims[0]; j++) {
            newExecutorState->datums[j] = Int16GetDatum(
                ((int16 *)(newExecutorState->colRawValues[i]))[j]);
          }
          ArrayType *array = construct_md_array(
              newExecutorState->datums, newExecutorState->nulls, 1, dims, lbs,
              INT2OID, sizeof(int16), true, 's');
          newExecutorState->colValues[i] = (Datum)(array);
        } else if (dataType == HAWQ_TYPE_INT4_ARRAY) {
          int dims[1];
          int lbs[1];
          dims[0] = newExecutorState->colValLength[i];
          lbs[0] = 1;
          if (newExecutorState->reserved < dims[0]) {
            newExecutorState->nulls = (bool *)repalloc(newExecutorState->nulls,
                                                       sizeof(bool) * dims[0]);
            newExecutorState->datums = (Datum *)repalloc(
                newExecutorState->datums, sizeof(Datum) * dims[0]);
          }
          int bitmask = 1;
          for (int j = 0; j < dims[0]; j++) {
            if (bitmask == 0x100) bitmask = 1;
            newExecutorState->nulls[j] =
                newExecutorState->colValNullBitmap[i][j / 8] & bitmask ? false
                                                                       : true;
            bitmask <<= 1;
          }
          for (int j = 0; j < dims[0]; j++) {
            newExecutorState->datums[j] = Int32GetDatum(
                ((int32 *)(newExecutorState->colRawValues[i]))[j]);
          }
          ArrayType *array = construct_md_array(
              newExecutorState->datums, newExecutorState->nulls, 1, dims, lbs,
              INT4OID, sizeof(int32), true, 'i');
          newExecutorState->colValues[i] = (Datum)(array);
        } else if (dataType == HAWQ_TYPE_INT8_ARRAY) {
          int dims[1];
          int lbs[1];
          dims[0] = newExecutorState->colValLength[i];
          lbs[0] = 1;
          if (newExecutorState->reserved < dims[0]) {
            newExecutorState->nulls = (bool *)repalloc(newExecutorState->nulls,
                                                       sizeof(bool) * dims[0]);
            newExecutorState->datums = (Datum *)repalloc(
                newExecutorState->datums, sizeof(Datum) * dims[0]);
          }
          int bitmask = 1;
          for (int j = 0; j < dims[0]; j++) {
            if (bitmask == 0x100) bitmask = 1;
            newExecutorState->nulls[j] =
                newExecutorState->colValNullBitmap[i][j / 8] & bitmask ? false
                                                                       : true;
            bitmask <<= 1;
          }
          ArrayType *array =
              construct_md_array((Datum *)(newExecutorState->colRawValues[i]),
                                 newExecutorState->nulls, 1, dims, lbs, INT8OID,
                                 sizeof(int64), true, 'd');
          newExecutorState->colValues[i] = (Datum)(array);
        } else if (dataType == HAWQ_TYPE_FLOAT4_ARRAY) {
          int dims[1];
          int lbs[1];
          dims[0] = newExecutorState->colValLength[i];
          lbs[0] = 1;
          if (newExecutorState->reserved < dims[0]) {
            newExecutorState->nulls = (bool *)repalloc(newExecutorState->nulls,
                                                       sizeof(bool) * dims[0]);
            newExecutorState->datums = (Datum *)repalloc(
                newExecutorState->datums, sizeof(Datum) * dims[0]);
          }
          int bitmask = 1;
          for (int j = 0; j < dims[0]; j++) {
            if (bitmask == 0x100) bitmask = 1;
            newExecutorState->nulls[j] =
                newExecutorState->colValNullBitmap[i][j / 8] & bitmask ? false
                                                                       : true;
            bitmask <<= 1;
          }
          for (int j = 0; j < dims[0]; j++) {
            newExecutorState->datums[j] = Float4GetDatum(
                ((float *)(newExecutorState->colRawValues[i]))[j]);
          }
          ArrayType *array = construct_md_array(
              newExecutorState->datums, newExecutorState->nulls, 1, dims, lbs,
              FLOAT4OID, sizeof(float4), true, 'i');
          newExecutorState->colValues[i] = (Datum)(array);
        } else if (dataType == HAWQ_TYPE_FLOAT8_ARRAY) {
          int dims[1];
          int lbs[1];
          dims[0] = newExecutorState->colValLength[i];
          lbs[0] = 1;
          if (newExecutorState->reserved < dims[0]) {
            newExecutorState->nulls = (bool *)repalloc(newExecutorState->nulls,
                                                       sizeof(bool) * dims[0]);
            newExecutorState->datums = (Datum *)repalloc(
                newExecutorState->datums, sizeof(Datum) * dims[0]);
          }
          int bitmask = 1;
          for (int j = 0; j < dims[0]; j++) {
            if (bitmask == 0x100) bitmask = 1;
            newExecutorState->nulls[j] =
                newExecutorState->colValNullBitmap[i][j / 8] & bitmask ? false
                                                                       : true;
            bitmask <<= 1;
          }
          ArrayType *array =
              construct_md_array((Datum *)(newExecutorState->colRawValues[i]),
                                 newExecutorState->nulls, 1, dims, lbs,
                                 FLOAT8OID, sizeof(float8), true, 'd');
          newExecutorState->colValues[i] = (Datum)(array);
        } else {
          newExecutorState->colValues[i] =
              *(Datum *)(newExecutorState->colRawValues[i]);
        }
      }
      /*
       * HAWQ_TYPE_TEXT, HAWQ_TYPE_BYTE, HAWQ_TYPE_BPCHAR, HAWQ_TYPE_VARCHAR
       */
      else {
        if (newExecutorState->colSpeedUp[i]) {
          newExecutorState->colValues[i] =
              newExecutorState->colRawValues[i] - VARHDRSZ;
          SET_VARSIZE((struct varlena *)(newExecutorState->colValues[i]),
                      newExecutorState->colValLength[i] + VARHDRSZ);
        } else {
          if (!last_batch_row) {
            if (dataType != HAWQ_TYPE_BYTE) {
              char *val = (char *)(newExecutorState->colRawValues[i]);
              char oldc = *(val + newExecutorState->colValLength[i]);
              *(val + newExecutorState->colValLength[i]) = '\0';
              newExecutorState->colValues[i] =
                  InputFunctionCall(&(newExecutorState->in_functions[i]), val,
                                    newExecutorState->typioparams[i],
                                    tupDesc->attrs[i]->atttypmod);
              *(val + newExecutorState->colValLength[i]) = oldc;
            } else {
              char *val =
                  (char *)palloc(newExecutorState->colValLength[i] + VARHDRSZ);
              SET_VARSIZE((struct varlena *)(val),
                          newExecutorState->colValLength[i] + VARHDRSZ);
              memcpy(val + VARHDRSZ, newExecutorState->colRawValues[i],
                     newExecutorState->colValLength[i]);
              newExecutorState->colValues[i] = val;
            }
          } else {
            if (dataType != HAWQ_TYPE_BYTE) {
              char *val = (char *)palloc(newExecutorState->colValLength[i] + 1);
              memcpy(val, newExecutorState->colRawValues[i],
                     newExecutorState->colValLength[i]);
              val[newExecutorState->colValLength[i]] = '\0';
              newExecutorState->colValues[i] =
                  InputFunctionCall(&(newExecutorState->in_functions[i]), val,
                                    newExecutorState->typioparams[i],
                                    tupDesc->attrs[i]->atttypmod);

              if (val) {
                pfree(val);
              }
            } else {
              char *val =
                  (char *)palloc(newExecutorState->colValLength[i] + VARHDRSZ);
              SET_VARSIZE((struct varlena *)(val),
                          newExecutorState->colValLength[i] + VARHDRSZ);
              memcpy(val + VARHDRSZ, newExecutorState->colRawValues[i],
                     newExecutorState->colValLength[i]);
              newExecutorState->colValues[i] = val;
            }
          }
        }
      }
    }

    /* form the tuple */
    newExecutorState->hasTuple = true;

    tts->PRIVATE_tts_values = newExecutorState->colValues;
    tts->PRIVATE_tts_isnull = newExecutorState->colIsNulls;
    ExecClearTuple(tts);
    ExecStoreVirtualTuple(tts);
  } else {
    newExecutorState->hasTuple = false;
  }
}

void endMyNewExecutor(MyNewExecutorTupState **newExecutorState) {
  MyExecutorEnd(MyNewExecutor);
  newExecutorPlanStateReference = NULL;
  ExecutorCatchedError *err = ExecutorGetLastError(MyNewExecutor);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    int errCode = err->errCode;
    if (Gp_role == GP_ROLE_DISPATCH) ExecutorFreeWorkHorse(MyNewExecutor);
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }
  ExecutorFreeWorkHorse(MyNewExecutor);
  pfree((*newExecutorState)->colRawValues);
  pfree((*newExecutorState)->colValues);
  pfree((*newExecutorState)->colValLength);
  pfree((*newExecutorState)->colValNullBitmap);
  pfree((*newExecutorState)->colAddresses);
  pfree((*newExecutorState)->colIsNulls);
  pfree((*newExecutorState)->colSpeedUpPossible);
  pfree((*newExecutorState)->colSpeedUp);
  pfree((*newExecutorState)->nulls);
  pfree((*newExecutorState)->datums);
  pfree((*newExecutorState)->in_functions);
  pfree((*newExecutorState)->typioparams);
  (*newExecutorState)->slot->PRIVATE_tts_values = NULL;
  (*newExecutorState)->slot->PRIVATE_tts_isnull = NULL;
  ExecClearTuple((*newExecutorState)->slot);
  pfree(*newExecutorState);
  *newExecutorState = NULL;
}

void stopMyNewExecutor() {
  MyExecutorStop(MyNewExecutor);
  ExecutorCatchedError *err = ExecutorGetLastError(MyNewExecutor);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    int errCode = err->errCode;
    if (Gp_role == GP_ROLE_DISPATCH) ExecutorFreeWorkHorse(MyNewExecutor);
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }
}

const char *getMyNewExecutorCompletionTag() {
  return MyExecutorGetCompletionTag(MyNewExecutor);
}

void resetMyNewExecutorCancelFlag() {
  if (MyNewExecutor) MyExecutorRestCancelFlag(MyNewExecutor);
}
