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
#ifndef __TUPLEBATCH_H__
#define __TUPLEBATCH_H__

#include "vexecutor.h"
#include "vcheck.h"
/*
 * --------
 * structure TupleBatchData can be seen as an extend component in TupleTableSlot.
 * It holds a batch of tuple in "datagroup" as well as stores vectorized execution
 * metadata.
 *
 * _tb_len is pg required variable. It indicates the total size of a TupleBatch object.
 * original serialization function checks it for memory allocation.
 *
 * batchsize represents the maxmium number of tuples could be stored in this structure.
 *
 * ncols and nrows indicates the real number of tuples data in the structure.
 * Instead of fully decoding, TupleBatch only decode used columns in a query.
 *
 * iter is an iterator for V->N process. For example, if a scanstate is vectorized and it parent
 * node does not. TupleTableSlot asks TupleBatch pop one single tuple as once invoked result.
 * The iter stores the number of tuples already processed.
 *
 * skip is mark array for tuple qualification. the size depend on nrows value.
 *
 * datagroup is a two dimential array for tuple datum literally. vheadr is abstruct class
 * for vectorized data type. All vtypes(e.g. vint2,vfloat8,vbool) inherits vheader
 *
 *
 *
 * For the construct function, tbGenerate, only build the main body of TupleBatch, which means
 * datagroup's secound level pointers are null in this step. Since it does not know which column
 * should be decoded and stored. User should create column buffer manually through tbCreateColumn.
 * Before decode tuple from disk, tbRset must be invoked to clean meta info including ncols, nrows and skip.
 * tbSerialization and tbDeserialization are the pair of serialization function.
 * --------
 */

typedef struct TupleBatchData
{
    int     batchsize;  //indicate maximum number of batch
    int     ncols;      //the number of target table column
    int     nrows;      //the number of tuples scaned out
    int     iter;       //used for data
    bool*   skip;       //used for qualification
    vtype** datagroup;
}TupleBatchData,*TupleBatch;

/* TupleBatch ctor */
TupleBatch tbGenerate(int colnum,int rownum);
/* init a specific column in TupleBatch depends on datum type */
void tbCreateColumn(TupleBatch tb,int colid,Oid type);
/* reset buffer and status in TupleBatch */
void tbReset(TupleBatch tb);
/* TupleBatch dtor */
void tbDestroy(TupleBatch* tb);
/* free one column */
void tbfreeColumn(vtype** vh,int colid);
/* TupleBatch serialization function */
MemTuple tbSerialization(TupleBatch tb);
/* TupleBatch deserialization function */
TupleBatch tbDeserialization(unsigned char *buffer);

#endif
