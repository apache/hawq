///////////////////////////////////////////////////////////////////////////////
// Copyright 2016, Oushu Inc.
// All rights reserved.
//
// Author:
///////////////////////////////////////////////////////////////////////////////

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_INSTRUMENT_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_INSTRUMENT_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct MyInstrumentation {
  struct MyInstrumentation *leftTree;
  struct MyInstrumentation *rightTree;
  struct MyInstrumentation *subTree;
  struct MyInstrumentation *subplan;
  struct MyInstrumentation *subplanSibling;

  bool running;              // TRUE if we've completed first tupleBatch
  uint64_t startTime;        // Start time of current iteration of node
  uint64_t counter;          // Accumulated runtime for this node
  uint64_t firstTupleBatch;  // Time for first tupleBatch of this cycle
  uint64_t firstStart;       // Start time of first iteration of node
  uint64_t tupleCount;       // Tuples emitted so far this cycle
  double nloops;             // Number of run cycles for this node

  double execmemused;    // CDB: executor memory used (bytes)
  double workmemused;    // CDB: work_mem actually used (bytes)
  double workmemwanted;  // CDB: work_mem to avoid scratch i/o (bytes)

  bool workfileReused;   // TRUE if cached workfiles reused in this node
  bool workfileCreated;  // TRUE if workfiles are created in this node

  uint32_t numPartScanned;  // Number of part tables scanned

  const char *notebuf;  // extra message text
  int32_t notebufLen;   // size of extra message text

  // for new scheduler only
  const char *queryId;
  int32_t stageNo;
  int32_t execId;
} MyInstrumentation;

__attribute__((weak)) void MyInstrInitNode(MyInstrumentation *instr) {}

__attribute__((weak)) void MyInstrStartNode(MyInstrumentation *instr) {}

__attribute__((weak)) void MyInstrStopNode(MyInstrumentation *instr, uint64_t nTuples) {}

__attribute__((weak)) void MyInstrEndLoop(MyInstrumentation *instr) {}

#ifdef __cplusplus
}
#endif

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_INSTRUMENT_H_
