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
#include "postgres.h"
#include "tuplebatch.h"

static size_t vtypeSize(vheader *vh);
static size_t tbSerializationSize(TupleBatch tb);

TupleBatch tbGenerate(int colnum,int batchsize)
{
    Assert(colnum > 0 && batchsize > 0);
    TupleBatch tb = palloc0(sizeof(TupleBatchData));
    if(!tb)
    {
        elog(FATAL,"TupleBatch Allocation failed");
        return NULL;
    }

    tb->ncols = colnum;
    tb->batchsize = batchsize;

    tb->skip = palloc0(sizeof(bool) * tb->batchsize);
    tb->datagroup = palloc0(sizeof(struct vtypeheader*) * tb->ncols);

    return tb;
}

void tbDestroy(TupleBatch* tb){
    free((*tb)->skip);
    for(int i = 0 ;i < (*tb)->ncols; ++i)
    {
        if((*tb)->datagroup[i])
            tbfreeColumn((*tb)->datagroup,i);
    }

    free((*tb)->datagroup);

    free((*tb));
    *tb = NULL;
}

void tbReset(TupleBatch tb)
{
    tb->iter = 0;
    tb->nrows = 0;
    memset(tb->skip,0, sizeof(bool) * tb->batchsize);
}

void tbCreateColumn(TupleBatch tb,int colid,Oid type)
{
    if(tb->ncols <= colid)
        return;
    int bs = tb->batchsize;

    GetVFunc(type)->vtbuild((bs));
}

void tbfreeColumn(vheader** vh,int colid)
{
    GetVFunc(vh[colid]->elemtype)->vtfree(&vh[colid]);
}

static size_t vtypeSize(vheader *vh)
{
    return GetVFunc(vh->elemtype)->vtsize(vh);
}

static size_t
tbSerializationSize(TupleBatch tb)
{
    //buffer size stick in the head of the buffer
    size_t len = sizeof(size_t);

    //get TupleBatch structure size
    len += offsetof(TupleBatchData ,skip);

    //get skip tag size
    len += sizeof( bool ) * tb->nrows;

    //get all un-null columns data size
    for(int i = 0;i < tb->ncols; i++ )
    {
        if(tb->datagroup[i])
        {
            len += sizeof(int);
            len += vtypeSize(tb->datagroup[i]);
        }
    }
    return len;
}

unsigned char *
tbSerialization(TupleBatch tb )
{
    size_t len = 0;
    size_t tmplen = 0;
    //calculate total size for TupleBatch
    size_t size = tbSerializationSize(tb);

    unsigned char *buffer = palloc(size);

    //copy TupleBatch header
    memcpy(buffer,&size,sizeof(size_t));

    tmplen = offsetof(TupleBatchData ,skip);
    memcpy(buffer+len,tb,tmplen);
    len += tmplen;

    tmplen = sizeof(bool) * tb->nrows;
    memcpy(buffer+len,tb->skip,tmplen);
    len += tmplen;


    for(int i = 0;i < tb->ncols; i++ )
    {
        if(tb->datagroup[i])
        {
            memcpy(buffer+len,&i,sizeof(int));
            len += sizeof(int);

            tmplen = GetVFunc(tb->datagroup[i]->elemtype)->serialization(tb->datagroup[i],buffer + len);
            len += tmplen;
        }
    }

    return buffer;
}

TupleBatch tbDeserialization(unsigned char *buffer)
{
    size_t buflen;
    memcpy(&buflen,buffer,sizeof(size_t));

    if(buflen < sizeof(TupleBatchData))
        return NULL;

    size_t len = 0;
    size_t tmplen = 0;
    TupleBatch tb = palloc0(sizeof(TupleBatchData));

    //deserial tb main data
    tmplen = offsetof(TupleBatchData,skip);
    memcpy(tb,buffer+len,tmplen);
    len += tmplen;

    //deserial member value -- skip
    if(tb->nrows != 0)
    {
        tb->skip = palloc(sizeof(bool) * tb->nrows);
        memcpy(tb->skip,buffer+len,tmplen);
        len += tmplen;
    }

    //deserial member value -- datagroup
    if(tb->ncols != 0)
    {
        tb->datagroup = palloc0(sizeof(vheader*) * tb->ncols);
        int colid;
        while (len < buflen)
        {
            memcpy(&colid,buffer + len,sizeof(int));
            len += sizeof(int);
            tb->datagroup[colid] = (vheader* ) GetVFunc(((vheader *) (buffer + len))->elemtype)->deserialization(buffer + len,&tmplen);
            len += tmplen;
        }
    }

    return tb;
}
