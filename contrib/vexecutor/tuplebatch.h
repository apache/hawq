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


typedef struct TupleBatchData
{
    int     _tb_len;
    int     batchsize;  //indicate maximum number of batch
    int     ncols;      //the number of target table column
    int     nrows;      //the number of tuples scaned out
    int     iter;       //used for data
    bool*   skip;       //used for qualification
    vheader** datagroup;
}TupleBatchData,*TupleBatch;

/* TupleBatch ctor */
TupleBatch tbGenerate(int colnum,int rownum);
/* init a specific column in TupleBatch depends on datum type */
void tbCreateColumn(TupleBatch tb,int colid,Oid type);
/* reset buffer and status in TupleBatch */
void tbReset(TupleBatch tb);
/* TupleBatch dtor */
void tbDestory(TupleBatch* tb);
/* free one column */
void tbfreeColumn(vheader** vh,int colid);
/* TupleBatch serialization function */
unsigned char * tbSerialization(TupleBatch tb);
/* TupleBatch deserialization function */
TupleBatch tbDeserialization(unsigned char *buffer);

#endif
