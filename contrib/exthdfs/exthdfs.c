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

#include "common.h"
#include "access/extprotocol.h"
#include "cdb/cdbdatalocality.h"
#include "utils/uri.h"

#include "storage/cwrapper/hdfs-file-system-c.h"
#include "cdb/cdbfilesystemcredential.h"
#include "cdb/cdbvars.h"
#include "postgres.h"

PG_MODULE_MAGIC
;

PG_FUNCTION_INFO_V1(hdfsprotocol_blocklocation);
PG_FUNCTION_INFO_V1(hdfsprotocol_validate);

Datum hdfsprotocol_blocklocation(PG_FUNCTION_ARGS);
Datum hdfsprotocol_validate(PG_FUNCTION_ARGS);

static char * getIpBySocket(const char * socket);
#define EXPECTED_MAX_HDFS_CONNECTIONS 10
Datum hdfsprotocol_blocklocation(PG_FUNCTION_ARGS)
{
	/* Build the result instance */
	ExtProtocolBlockLocationData *bldata = palloc0(
			sizeof(ExtProtocolBlockLocationData));
	if (bldata == NULL)
	{
		elog(ERROR, "hdfsprotocol_blocklocation : "
		"cannot allocate due to no memory");
	}
	bldata->type = T_ExtProtocolBlockLocationData;
	fcinfo->resultinfo = bldata;

	ExtProtocolValidatorData *pvalidator_data =
			(ExtProtocolValidatorData *) (fcinfo->context);

	/*
	 * Parse URI of the first location, we expect all locations uses the same
	 * name node server. This is checked in validation function.
	 */
	char *first_uri_str = (char *) strVal(
			lfirst(list_head(pvalidator_data->url_list)));
	Uri *uri = ParseExternalTableUri(first_uri_str);

	elog(DEBUG3, "hdfsprotocol_blocklocation : "
	"extracted HDFS name node address %s:%d",
	uri->hostname, uri->port);

	if (enable_secure_filesystem && Gp_role != GP_ROLE_EXECUTE)
	{
		char *ccname = NULL;
		/*
		 * refresh kerberos ticket
		 */
		if (!login())
		{
			errno = EACCES;
		}
		ccname = pstrdup(krb5_ccname);
		SetCcname(ccname);
		if (ccname)
		  pfree(ccname);
	}

	/* Create file system instance */
	FscHdfsFileSystemC *fs = FscHdfsNewFileSystem(uri->hostname, uri->port);
	if (fs == NULL)
	{
		elog(ERROR, "hdfsprotocol_blocklocation : "
		"failed to create HDFS instance to connect to %s:%d",
		uri->hostname, uri->port);
	}

	/* Clean up uri instance as we don't need it any longer */
	FreeExternalTableUri(uri);

	/* Check all locations to get files to fetch location. */
	ListCell *lc = NULL;
	foreach(lc, pvalidator_data->url_list)
	{
		/* Parse current location URI. */
		char *url = (char *) strVal(lfirst(lc));
		Uri *uri = ParseExternalTableUri(url);
		if (uri == NULL)
		{
			elog(ERROR, "hdfsprotocol_blocklocation : "
			"invalid URI encountered %s", url);
		}

		/*
		 * NOTICE: We temporarily support only directories as locations. We plan
		 *         to extend the logic to specifying single file as one location
		 *         very soon.
		 */

		/* get files contained in the path. */
		FscHdfsFileInfoArrayC *fiarray = FscHdfsDirPath(fs, uri->path);
		if (FscHdfsHasErrorRaised(fs))
		{
			Assert(fiarray == NULL);
			CatchedError *ce = FscHdfsGetFileSystemError(fs);
			elog(ERROR, "hdfsprotocol_blocklocation : "
			"failed to get files of path %s. %s (%d)",
			uri->path,
			ce->errMessage, ce->errCode);
		}

		/* Call block location api to get data location for each file */
		for (int i = 0; true; i++)
		{
			FscHdfsFileInfoC *fi = FscHdfsGetFileInfoFromArray(fiarray, i);

			/* break condition of this for loop */
			if (fi == NULL)
			{
				break;
			}

			/* Build file name full path. */
			const char *fname = FscHdfsGetFileInfoName(fi);
			char *fullpath = palloc0(strlen(uri->path) + /* path  */
			1 + /* slash */
			strlen(fname) + /* name  */
			1); /* \0    */
			sprintf(fullpath, "%s/%s", uri->path, fname);

			elog(DEBUG3, "hdfsprotocol_blocklocation : "
			"built full path file %s", fullpath);

			/* Get file full length. */
			int64_t len = FscHdfsGetFileInfoLength(fi);

			elog(DEBUG3, "hdfsprotocol_blocklocation : "
			"got file %s length " INT64_FORMAT,
			fullpath, len);

			if (len == 0)
			{
				pfree(fullpath);
				continue;
			}

			/* Get block location data for this file */
			FscHdfsFileBlockLocationArrayC *bla =
					FscHdfsGetPathFileBlockLocation(fs, fullpath, 0, len);
			if (FscHdfsHasErrorRaised(fs))
			{
				Assert(bla == NULL);
				CatchedError *ce = FscHdfsGetFileSystemError(fs);
				elog(ERROR, "hdfsprotocol_blocklocation : "
				"failed to get block location of path %s. "
				"It is reported generally due to HDFS service errors or "
				"another session's ongoing writing.",
				fullpath);
			}

			/* Add file full path and its block number as result. */
			blocklocation_file *blf = palloc0(sizeof(blocklocation_file));
			blf->file_uri = pstrdup(fullpath);
			blf->block_num = FscHdfsGetFileBlockLocationArraySize(bla);
			blf->locations = palloc0(sizeof(BlockLocation) * blf->block_num);

			elog(DEBUG3, "hdfsprotocol_blocklocation : file %s has %d blocks",
			fullpath, blf->block_num);

			/* We don't need it any longer */
			pfree(fullpath);

			/* Add block information as a list. */
			for (int bidx = 0; bidx < blf->block_num; bidx++)
			{
				FscHdfsFileBlockLocationC *blo =
						FscHdfsGetFileBlockLocationFromArray(bla, bidx);
				BlockLocation *bl = &(blf->locations[bidx]);
				bl->numOfNodes = FscHdfsGetFileBlockLocationNNodes(blo);
				bl->rangeId = -1;
				bl->replicaGroupId = -1;
				bl->hosts = (char **) palloc0(sizeof(char *) * bl->numOfNodes);
				bl->names = (char **) palloc0(sizeof(char *) * bl->numOfNodes);
				bl->topologyPaths = (char **) palloc0(
						sizeof(char *) * bl->numOfNodes);
				bl->offset = FscHdfsGetFileBlockLocationOffset(blo);
				bl->length = FscHdfsGetFileBlockLocationLength(blo);
				bl->corrupt = FscHdfsGetFileBlockLocationCorrupt(blo);

				for (int nidx = 0; nidx < bl->numOfNodes; nidx++)
				{
					bl->hosts[nidx] = pstrdup(
							FscHdfsGetFileBlockLocationNodeHost(blo, nidx));
					bl->names[nidx] = pstrdup(
							FscHdfsGetFileBlockLocationNodeName(blo, nidx));
//					elog (LOG,"the host of bidx %d nidx %d is %s, name is %s.",bidx, nidx, bl->hosts[nidx], bl->names[nidx]);
					bl->topologyPaths[nidx] = pstrdup(
							FscHdfsGetFileBlockLocationNodeTopoPath(blo, nidx));
				}
			}

			bldata->files = lappend(bldata->files, (void *) (blf));

			/* Clean up block location instances created by the lib. */
			FscHdfsFreeFileBlockLocationArrayC(&bla);
		}

		/* Clean up URI instance in loop as we don't need it any longer */
		FreeExternalTableUri(uri);

		/* Clean up file info array created by the lib for this location. */
		FscHdfsFreeFileInfoArrayC(&fiarray);
	}

	/* destroy fs instance */
	FscHdfsFreeFileSystemC(&fs);
	PG_RETURN_VOID() ;
}

Datum hdfsprotocol_validate(PG_FUNCTION_ARGS)
{
  if(nodeTag(fcinfo->context) == T_ExtProtocolRenameData) {
    if (enable_secure_filesystem && Gp_role != GP_ROLE_EXECUTE)
    {
      char *ccname = NULL;
      /*
       * refresh kerberos ticket
       */
      if (!login())
      {
        errno = EACCES;
      }
      ccname = pstrdup(krb5_ccname);
      SetCcname(ccname);
      if (ccname)
        pfree(ccname);
    }
    ExtProtocolRenameData *renameData= (ExtProtocolRenameData *)fcinfo->context;
    FscHdfsFileSystemC *fs = FscHdfsNewFileSystem(renameData->olduri->hostname,
                                                  renameData->olduri->port);
    if (fs == NULL) {
        elog(ERROR, "RenamePathInHDFS : "
        "failed to create HDFS instance to connect to %s:%d",
        renameData->olduri->hostname, renameData->olduri->port);
      }
    FscHdfsRenamePath(fs, renameData->olduri->path, renameData->newduri->path);
    FscHdfsFreeFileSystemC(&fs);
    PG_RETURN_VOID() ;
  }
	elog(DEBUG3, "hdfsprotocol_validate() begin");

	/* Check which action should perform. */
	ExtProtocolValidatorData *pvalidator_data =
			(ExtProtocolValidatorData *) (fcinfo->context);

	if (pvalidator_data->forceCreateDir)
		Assert(pvalidator_data->url_list && pvalidator_data->url_list->length == 1);

	if (pvalidator_data->direction == EXT_VALIDATE_WRITE)
	{
		/* accept only one directory location */
		if (list_length(pvalidator_data->url_list) != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "only one location url is supported for writable external hdfs")));
		}
	}
	char *first_uri_str = (char *) strVal(
			lfirst(list_head(pvalidator_data->url_list)));
	Uri *uri = ParseExternalTableUri(first_uri_str);

	/* Go through first round to get formatter type */
	bool isCsv = false;
	bool isText = false;
	bool isOrc = false;
	ListCell *optcell = NULL;

	foreach(optcell, pvalidator_data->format_opts)
	{
		DefElem *de = (DefElem *) lfirst(optcell);
		if (strcasecmp(de->defname, "formatter") == 0)
		{
			char *val = strVal(de->arg);
			if (strcasecmp(val, "csv") == 0)
			{
				isCsv = true;
			}
			else if (strcasecmp(val, "text") == 0)
			{
				isText = true;
			}
			else if (strcasecmp(val, "orc") == 0)
			{
				isOrc = true;
			}
		}
	}
	if (!isCsv && !isText && !isOrc)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "only 'csv', 'text' and 'orc' formatter is supported for external hdfs")));
	}Assert(isCsv || isText || isOrc);

	/* Validate formatter options */
	foreach(optcell, pvalidator_data->format_opts)
	{
		DefElem *de = (DefElem *) lfirst(optcell);
		if (strcasecmp(de->defname, "delimiter") == 0)
		{
			char *val = strVal(de->arg);
			/* Validation 1. User can not specify 'OFF' in delimiter */
			if (strcasecmp(val, "off") == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'off' value of 'delimiter' option is not supported")));
			}
			/* Validation 2. Can specify multibytes characters */
			if (strlen(val) < 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'delimiter' option accepts multibytes characters")));
			}
		}

		if (strcasecmp(de->defname, "escape") == 0)
		{
			char *val = strVal(de->arg);
			/* Validation 3. User can not specify 'OFF' in escape except for TEXT format */
		  if (strcasecmp(val, "off") == 0 && !isText)
      {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'off' value of 'escape' option is not supported")));
      }
			/* Validation 4. Can only specify one character */
			if (strlen(val) != 1 && strcasecmp(val, "off") != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'escape' option accepts single character")));
			}
		}

		if (strcasecmp(de->defname, "newline") == 0)
		{
			char *val = strVal(de->arg);
			/* Validation 5. only accept 'lf', 'cr', 'crlf' */
			if (strcasecmp(val, "lf") != 0 && strcasecmp(val, "cr") != 0
					&& strcasecmp(val, "crlf") != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "the value of 'newline' option can only be " "'lf', 'cr' or 'crlf'")));
			}
		}

		if (strcasecmp(de->defname, "quote") == 0)
		{
			/* This is allowed only for csv mode formatter */
			if (!isCsv)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'quote' option is only available in 'csv' formatter")));
			}

			char *val = strVal(de->arg);
			/* Validation 5. Can only specify one character */
			if (strlen(val) != 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'quote' option accepts single character")));
			}
		}

		if (strcasecmp(de->defname, "force_notnull") == 0)
		{
			/* This is allowed only for csv mode formatter */
			if (!isCsv)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'force_notnull' option is only available in 'csv' formatter")));
			}
		}

		if (strcasecmp(de->defname, "force_quote") == 0)
		{
			/* This is allowed only for csv mode formatter */
			if (!isCsv)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'force_quote' option is only available in 'csv' formatter")));
			}
		}

		if (strcasecmp(de->defname, "header") == 0)
		{
		  /* this is allowed only for readable table */
		  if (pvalidator_data->direction != EXT_VALIDATE_READ)
		  {
		    ereport(ERROR,
		        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("hdfsprotocol_validate : " "'header' option is only available in readable external table")));
		  }
		}
	}

	/* All urls should
	 * 1) have the same protocol name 'hdfs',
	 * 2) the same hdfs namenode server address
	 */
	/* Check all locations to get files to fetch location. */
	char *nnaddr = NULL;
	int nnport = -1;
	ListCell *lc = NULL;
	foreach(lc, pvalidator_data->url_list)
	{
		/* Parse current location URI. */
		char *url = (char *) strVal(lfirst(lc));
		Uri *uri = ParseExternalTableUri(url);
		if (uri == NULL)
		{
			elog(ERROR, "hdfsprotocol_validate : "
			"invalid URI encountered %s", url);
		}

		if (uri->protocol != URI_HDFS)
		{
			elog(ERROR, "hdfsprotocol_validate : "
			"invalid URI protocol encountered in %s, "
			"hdfs:// protocol is required",
			url);
		}

		if(uri->path[1] == '/'){
		  elog(ERROR, "hdfsprotocol_validate : "
		      "invalid files path in %s", uri->path);
		}

		if (nnaddr == NULL)
		{
			nnaddr = pstrdup(uri->hostname);
			nnport = uri->port;
		}
		else
		{
			if (strcmp(nnaddr, uri->hostname) != 0)
			{
				elog(ERROR, "hdfsprotocol_validate : "
				"different name server addresses are detected, "
				"both %s and %s are found",
				nnaddr, uri->hostname);
			}
			if (nnport != uri->port)
			{
				elog(ERROR, "hdfsprotocol_validate : "
				"different name server ports are detected, "
				"both %d and %d are found",
				nnport, uri->port);
			}
		}

		/* SHOULD ADD LOGIC HERE TO CREATE UNEXISTING PATH */
		if (pvalidator_data->forceCreateDir)
		{

			elog(LOG, "hdfs_validator() forced creating dir");
			if (enable_secure_filesystem && Gp_role != GP_ROLE_EXECUTE)
			{
				char *ccname = NULL;
				/*
				 * refresh kerberos ticket
				 */
				if (!login())
				{
					errno = EACCES;
				}
				ccname = pstrdup(krb5_ccname);
				SetCcname(ccname);
				if (ccname)
				  pfree(ccname);
			}

			/* Create file system instance */
			FscHdfsFileSystemC *fs = FscHdfsNewFileSystem(uri->hostname,
					uri->port);
			if (fs == NULL)
			{
				elog(ERROR, "hdfsprotocol_validate : "
				"failed to create HDFS instance to connect to %s:%d",
				uri->hostname, uri->port);
			}

			if (FscHdfsExistPath(fs, uri->path)
					&& FscHdfsGetFileKind(fs, uri->path) == 'F')
				elog(ERROR, "hdfsprotocol_validate : "
				"Location \"%s\" is a file, not supported yet. "
				"Only support directory now",
				uri->path);
			if (pvalidator_data->direction == EXT_VALIDATE_WRITE
					&& !FscHdfsExistInsertPath(fs, uri->path))
			{
				elog(LOG, "hdfs_validator() to create url %s", uri->path);
				FscHdfsCreateInsertDir(fs, uri->path);
				if (FscHdfsHasErrorRaised(fs))
				{
					CatchedError *ce = FscHdfsGetFileSystemError(fs);
					elog(ERROR, "hdfsprotocol_validate : "
					"failed to create directory %s : %s(%d)",
					uri->path,
					ce->errMessage, ce->errCode);
				}
			}

			/* destroy fs instance */
			FscHdfsFreeFileSystemC(&fs);
		}

		/* Clean up temporarily created instances */
		FreeExternalTableUri(uri);
		if (nnaddr != NULL)
		{
			pfree(nnaddr);
		}
	}

	elog(LOG, "passed validating hdfs protocol options");

	/**************************************************************************
	 * This is a bad implementation that we check formatter options here. Should
	 * be moved to call formatter specific validation UDFs.
	 **************************************************************************/

	PG_RETURN_VOID() ;
}

static char * getIpBySocket(const char * socket) {
  if (socket == NULL) {
    return NULL;
  }
  int len = 0;
  while (socket[len] != ':' && socket[len] != '\0') {
    len++;
  }
  return pnstrdup(socket,len);
}

