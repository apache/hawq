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
    pfree((*tb)->skip);
    for(int i = 0 ;i < (*tb)->ncols; ++i)
    {
        if((*tb)->datagroup[i])
            tbfreeColumn((*tb)->datagroup,i);
    }

    pfree((*tb)->datagroup);

    pfree((*tb));
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

    tb->datagroup[colid] = buildvtype(type,bs,tb->skip);
}

void tbfreeColumn(vtype** vh,int colid)
{
    destroyvtype(&vh[colid]);
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

    int vtypeSz = VTYPESIZE(tb->nrows);
    //get all un-null columns data size
    for(int i = 0;i < tb->ncols; i++ )
    {
        if(tb->datagroup[i])
        {
            len += sizeof(int);
            len += vtypeSz;
        }
    }
    return len;
}

MemTuple
tbSerialization(TupleBatch tb )
{
    MemTuple ret;
    size_t len = 0;
    size_t tmplen = 0;
    //calculate total size for TupleBatch
    size_t size = tbSerializationSize(tb);
    //makes buffer length about 8-bytes alignment for motion
    size = (size + 0x8) & (~0x7);

    ret = palloc0(size);
    unsigned char *buffer = ret->PRIVATE_mt_bits;

    //copy TupleBatch header
    memcpy(buffer,&size,sizeof(size_t));
    buffer += sizeof(size_t);

    tmplen = offsetof(TupleBatchData ,skip);
    memcpy(buffer,tb,tmplen);
    buffer +=tmplen;

    tmplen = sizeof(bool) * tb->nrows;
    memcpy(buffer,tb->skip,tmplen);
    buffer += tmplen;


    for(int i = 0;i < tb->ncols; i++ )
    {
        if(tb->datagroup[i])
        {
            memcpy(buffer,&i,sizeof(int));
            buffer += sizeof(int);

            unsigned char* ptr = buffer;
            memcpy(ptr,tb->datagroup[i],offsetof(vtype,isnull));
            ptr+= offsetof(vtype,isnull);

            tmplen = VDATUMSZ(tb->nrows);
            memcpy(ptr,tb->datagroup[i]->values, tmplen);
            ptr += tmplen;

            tmplen = ISNULLSZ(tb->nrows);
            memcpy(ptr,tb->datagroup[i]->isnull,tmplen);
            buffer += VTYPESIZE(tb->nrows);
        }
    }

    memtuple_set_size(ret,NULL,size);
    return ret;
}

TupleBatch tbDeserialization(unsigned char *buffer)
{
    size_t buflen;
    size_t len = 0;
    size_t tmplen = 0;
    tmplen = sizeof(size_t);
    memcpy(&buflen,buffer,tmplen);
    len += tmplen;

    if(buflen < sizeof(TupleBatchData))
        return NULL;

    TupleBatch tb = palloc0(sizeof(TupleBatchData));

    //deserial tb main data
    tmplen = offsetof(TupleBatchData,skip);
    memcpy(tb,buffer + len,tmplen);
    len += tmplen;

    //deserial member value -- skip
    if(tb->nrows != 0)
    {
        tmplen = sizeof(bool) * tb->nrows;
        tb->skip = palloc(tmplen);
        memcpy(tb->skip,buffer+len,tmplen);
        len += tmplen;
    }

    //deserial member value -- datagroup
    if(tb->ncols != 0)
    {
        int colid;
        tmplen = sizeof(vtype*) * tb->ncols;
        tb->datagroup = palloc0(tmplen);
        //the buffer length is 8-bytes alignment, 
        //so we need align the current length before comparing.
        while (((len + 0x8) & (~0x7)) < buflen)
        {
            memcpy(&colid,buffer + len,sizeof(int));
            len += sizeof(int);

            vtype* src = (vtype*)(buffer + len);
            tb->datagroup[colid] = buildvtype(src->elemtype,tb->batchsize,tb->skip);

            tmplen = VDATUMSZ(tb->batchsize);
            //in vtype pointer isnull and skipref are't serialized
            memcpy(tb->datagroup[colid]->values,src->values - 2,tmplen);

            memcpy(tb->datagroup[colid]->isnull,ISNULLOFFSET(src) - 2,tb->nrows * sizeof(bool));

            len += VTYPESIZE(tb->nrows);
        }
    }

    return tb;
}
