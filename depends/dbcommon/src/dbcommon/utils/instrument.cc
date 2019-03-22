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

#include "dbcommon/utils/instrument.h"

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/time-util.h"

void MyInstrInitNode(MyInstrumentation *instr) {
  instr->leftTree = nullptr;
  instr->rightTree = nullptr;
  instr->subTree = nullptr;
  instr->subplan = nullptr;
  instr->subplanSibling = nullptr;
  instr->running = false;
  instr->startTime = 0;
  instr->counter = 0;
  instr->firstTupleBatch = 0;
  instr->firstStart = 0;
  instr->tupleCount = 0;
  instr->nloops = 0;
  instr->execmemused = 0;
  instr->workmemused = 0;
  instr->workmemwanted = 0;
  instr->workfileReused = false;
  instr->workfileCreated = false;
  instr->numPartScanned = 0;
  instr->notebuf = nullptr;
  instr->notebufLen = 0;
}

void MyInstrStartNode(MyInstrumentation *instr) {
  if (instr->startTime == 0)
    instr->startTime = dbcommon::TimeUtil::currentTime();
  else
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "instrStartNode called twice in a batch.");
}

void MyInstrStopNode(MyInstrumentation *instr, uint64_t nTuples) {
  // count the returned tuples
  instr->tupleCount += nTuples;

  if (instr->startTime == 0) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "instrStopNode called without start.");
  }

  uint64_t endTime = dbcommon::TimeUtil::currentTime();
  instr->counter += endTime - instr->startTime;

  // Is this the first tupleBatch of this cycle?
  if (!instr->running) {
    instr->running = true;
    instr->firstTupleBatch = instr->counter;
    instr->firstStart = instr->startTime;
  }

  instr->startTime = 0;
}

void MyInstrEndLoop(MyInstrumentation *instr) {
  if (!instr->running) return;
  instr->nloops += 1;
}
