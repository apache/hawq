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

#include <unistd.h>

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "cdb/cdbsharedoidsearch.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbvars.h"
#include "storage/itemptr.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/aomd.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/pmsignal.h"           /* PostmasterIsAlive */
#include "storage/sinvaladt.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "commands/tablespace.h"
#include "commands/dbcommands.h"
#include "cdb/cdbmetadatacache.h"
#include "tcop/tcopprot.h" /* quickdie() */
#include "storage/backendid.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "utils/ps_status.h"
#include "libpq/pqsignal.h"
#include "utils/memutils.h"

#include "funcapi.h"
#include "fmgr.h"
#include "utils/builtins.h"

#define MAX_FILENAME_LEN            1024

typedef struct MetadataCacheTestKey
{
    char                filename[MAX_FILENAME_LEN];
} MetadataCacheTestKey;

typedef struct BlockLocationTestEntry
{
    MetadataCacheTestKey key;
    BlockLocation       *locations;
    uint64_t            file_size;
    uint32_t            block_size;
    uint32_t            block_num;
} BlockLocationTestEntry;

static HTAB         *MetadataCacheForTest = NULL;
static char         MetadataCacheTestFile[MAX_FILENAME_LEN] = {'\0'};

static void 
InitMetadataCacheTestKey(MetadataCacheTestKey *key, const char *filename);

static void 
InitMetadataCacheForTest(void);

static void 
DestroyMetadataCacheForTest(void);

void 
InitMetadataCacheTestKey(MetadataCacheTestKey *key, const char *filename)
{
    Insist(NULL != key);
    Insist(NULL != filename);

    snprintf(key->filename, MAX_FILENAME_LEN, "%s", filename);
}

void DestroyMetadataCacheForTest(void)
{
    HASH_SEQ_STATUS hstat;
    BlockLocationTestEntry *entry; 
    MemoryContext old = CurrentMemoryContext;

    if (MetadataCacheForTest)
    {
        MemoryContextSwitchTo(TopMemoryContext);

        hash_seq_init(&hstat, MetadataCacheForTest);
        
        while ((entry = (BlockLocationTestEntry *)hash_seq_search(&hstat)) != NULL)
        {
            FreeHdfsFileBlockLocations(entry->locations, entry->block_num); 
        }

        hash_destroy(MetadataCacheForTest);
        MetadataCacheForTest = NULL;
        
        MemoryContextSwitchTo(old);
    }
}

void InitMetadataCacheForTest(void)
{
    HASHCTL ctl;
    ctl.keysize = sizeof(MetadataCacheTestKey);
    ctl.entrysize = sizeof(BlockLocationTestEntry);
    ctl.hcxt = TopMemoryContext;
    MetadataCacheForTest = hash_create("Metadata Cache For Test", 1024, &ctl, HASH_ELEM);

    // load fake file
    FILE *testfile = fopen(metadata_cache_testfile, "r");
    if (!testfile)
    {
        elog(ERROR, "InitMetadataCacheForTest invalid testfile:%s", metadata_cache_testfile);
    }
    else
    {
        elog(LOG, "InitMetadataCacheForTest load testfile:%s successfully", metadata_cache_testfile);
    }

    MemoryContext old = CurrentMemoryContext;
    MetadataCacheTestKey key;
    BlockLocationTestEntry *entry;
    bool found;
    char line[1024];
    char tmp[1024];
    char *data;
    uint32_t block_idx = 0;
    int i;
    
    MemoryContextSwitchTo(TopMemoryContext);
    while (fgets(line, 1024, testfile) !=NULL)
    {
        if (line[strlen(line)-1] == '\n')
        {
            line[strlen(line)-1] = '\0';
        }

        if (strncmp(line, "filename:", strlen("filename:")) == 0)
        {
            data = line + strlen("filename:");

            InitMetadataCacheTestKey(&key, data);
            entry = (BlockLocationTestEntry *)hash_search(MetadataCacheForTest, (void *)&key, HASH_ENTER, &found);
            if (!entry)
            {
                elog(ERROR, "InitMetadataCacheForTest hash enter error. filename:%s", data);
            }

            elog(LOG, "InitMetadataCacheForTest filename:%s", data);
        }
        else if (strncmp(line, "fileSize:", strlen("fileSize:")) == 0)
        {
            Insist(entry != NULL);
            data = line + strlen("fileSize:");
            entry->file_size = strtoull(data, NULL, 0); 
        }
        else if (strncmp(line, "blockSize:", strlen("blockSize:")) == 0)
        {
            Insist(entry != NULL);
            data = line + strlen("blockSize:");
            entry->block_size = strtoull(data, NULL, 0);

            entry->block_num = entry->file_size / entry->block_size;
            if (entry->file_size - entry->block_size * entry->block_num > 0)
            {
                entry->block_num++;
            }

            entry->locations = (BlockLocation *)palloc(sizeof(BlockLocation) * entry->block_num);
            for (i=0;i<entry->block_num;i++)
            {
                entry->locations[i].offset = entry->block_size * i;
                entry->locations[i].length = entry->block_size;
            }
            entry->locations[entry->block_num-1].length = entry->file_size % entry->block_size ? entry->file_size % entry->block_size : entry->block_size;
        }
        else
        {
            Insist(entry != NULL);
            // block list
            if (block_idx < entry->block_num)
            {
                entry->locations[block_idx].corrupt = 0; 
                entry->locations[block_idx].numOfNodes = 0; 
                strcpy(tmp, line);
                for(data=strtok(tmp, ",");data;data=strtok(NULL, ","))
                {
                    entry->locations[block_idx].numOfNodes++; 
                }

                entry->locations[block_idx].hosts = (char **)palloc(sizeof(char *) * entry->locations[block_idx].numOfNodes);
                entry->locations[block_idx].names = (char **)palloc(sizeof(char *) * entry->locations[block_idx].numOfNodes);
                entry->locations[block_idx].topologyPaths = (char **)palloc(sizeof(char *) * entry->locations[block_idx].numOfNodes);
                strcpy(tmp, line);
                for(i=0,data=strtok(tmp, ",");data;data=strtok(NULL, ","),i++)
                {
                    entry->locations[block_idx].hosts[i] = pstrdup(data);
                    entry->locations[block_idx].names[i] = pstrdup(data);
                    entry->locations[block_idx].topologyPaths[i] = pstrdup(data);
                }

                block_idx++;
                if (block_idx == entry->block_num)
                {
                    // last block
                    block_idx = 0;
                }
            }
        }
    }
    MemoryContextSwitchTo(old);

    if (testfile)
    {
        fclose(testfile);
    }
}

BlockLocation *GetHdfsFileBlockLocationsForTest(const char *filepath, uint64_t filesize, int *block_num)
{
    Insist(metadata_cache_testfile != NULL);
    Insist(strlen(metadata_cache_testfile) > 0 && strlen(metadata_cache_testfile) < MAX_FILENAME_LEN);

    if (strcmp(metadata_cache_testfile, MetadataCacheTestFile) != 0)
    {
        // test file update
        snprintf(MetadataCacheTestFile, MAX_FILENAME_LEN, "%s", metadata_cache_testfile);
        DestroyMetadataCacheForTest();
        InitMetadataCacheForTest(); 
    }

    MetadataCacheTestKey key;
    BlockLocationTestEntry *entry;
    bool found;

    InitMetadataCacheTestKey(&key, filepath);
    entry = (BlockLocationTestEntry *)hash_search(MetadataCacheForTest, (void *)&key, HASH_FIND, &found);

    if (found)
    {
        *block_num = entry->block_num;
        return CreateHdfsFileBlockLocations(entry->locations, entry->block_num);
    }
    else
    {
        return NULL;
    }
}

